module MattDaemon

##### stdlib
using Dates
using Serialization
using Sockets

##### Internal Dependencies
# These are dependencies that I created and am responsible for.
using SystemSnoop
using CounterTools

##### External Dependencies
using MacroTools

#####
##### Macro for wrapping measurements
#####

struct Preload
    f::Any
    args::Any
end
(P::Preload)() = (P.f)(P.args...)

"""
    @measurements measurements::NamedTuple

Wrap up `nt` such that it can be serialized and sent across to `MattDaemon` for
compatibilty with `SystemSnoop`.
"""
macro measurements(ex)
    # Make sure that a tuple is passed
    if ex.head != :tuple
        throw(error("Expected a NamedTuple"))
    end

    # Step through each argument, make sure it's a NamedTuple
    if any(!isequal(:(=)), map(x -> x.head, ex.args))
        throw(error("Expected a NamedTuple"))
    end

    # Slurp up the named tuple names
    names = map(x -> QuoteNode(first(x.args)), ex.args)

    # Slurp up the named tuple values
    values = map(x -> x.args[2], ex.args)

    # Convert any function calls into `Payload`s
    values = map(values) do _ex
        if MacroTools.@capture(_ex, f_(args__))
            return :(Preload($(esc(f)), $(esc(args))))
        else
            return esc(_ex)
        end
    end

    # Build up the named tuple expression
    return quote
        NamedTuple{($(names...),)}(($(values...),))
    end
end

function materialize(x::NamedTuple{names}) where {names}
    return NamedTuple{names}((materialize.(Tuple(x))...,))
end
materialize(x::Preload) = (x.f)(x.args...)
materialize(x) = x

#####
##### Expected Payload
#####

struct ServerPayload
    sampletime::Int64
    measurements::Any
end

#####
##### Glue
#####

# Make CounterTools compatible with SystemSnoop
SystemSnoop.measure(monitor::CounterTools.CoreMonitor) = read(monitor)
SystemSnoop.measure(monitor::CounterTools.IMCMonitor) = read(monitor)

#####
##### Set up some
#####

function runserver(port)
    # Setup a sever listening to the Named Pipe
    # Set the permissions on the server so it is readable and writable by non-sudo processes.
    server = listen(port)

    # Run forever!
    while true
        sock = accept(server)
        payload = nothing

        # Wait for a "go" method
        while isopen(sock)
            cmd = readline(sock)

            # Payload transmission
            if cmd == "payload"
                payload = deserialize(sock)

            # Start recording
            elseif cmd == "start"
                sample(sock, payload)

            # Who know what happene.
            else
                println("Unknown Command: $cmd")
            end
        end
    end
end

sample(sock, ::Nothing) = nothing
function sample(sock, payload)
    # Set up a smart sampler for regular updates
    sampler = SystemSnoop.SmartSample(Second(payload.sampletime))

    local trace
    @sync begin
        # Spawn a task to sample the buffer and notify when a `stop` command is reached.
        canexit = false
        @async begin
            while true
                ln = readline(sock)
                if ln == "stop"
                    canexit = true
                    break
                else
                    println("Unhandled command: $ln")
                end
            end
        end

        # Measuring loop
        trace = SystemSnoop.snoop(materialize(payload.measurements)) do snooper
            while true
                # Sleep until it's time to sample
                sleep(sampler)
                SystemSnoop.measure!(snooper)

                # Check to see if we can exit.
                if canexit
                    println("Stopping Sampling")
                    return nothing
                end
            end
        end
    end

    # Now that we've finished taking measurements, send the trace back along the pipe.
    serialize(sock, trace)
    return nothing
end

end # module
