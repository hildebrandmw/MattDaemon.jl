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

# I use this FunctionWrapper type to essentially encode a lazy function call.
#
# This is because serializing Closures from programs communicating with MattDaemon
# run into WorldAge issues.
#
# So instead, we invoke functions that are from the correct WorldAge so MattDaemon
# can run them.
#
# See tests for usage.
struct FunctionWrapper
    f::Any
    args::Any
    kw::NamedTuple
end
Base.:(==)(a::FunctionWrapper, b::FunctionWrapper) = (a.f == b.f) && (a.args == b.args) && (a.kw == b.kw)

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
        if MacroTools.@capture(_ex, f_(args__; kw__))
            args = esc.(args)
            kw = esc.(kw)
            return :(FunctionWrapper($(esc(f)), [$(args...)], (;$(kw...))))
        elseif MacroTools.@capture(_ex, f_(args__))
            args = esc.(args)
            return :(FunctionWrapper($(esc(f)), [$(args...)], NamedTuple()))
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
materialize(x::FunctionWrapper) = (x.f)(x.args...; x.kw...)
materialize(x) = x

#####
##### Expected Payload
#####

struct ServerPayload
    sampletime::TimePeriod
    measurements::Any
end

# Server API
function ping(io::IO; timeout = Second(10))
    println(io, "ping")
    return readline(io) == "ping"
end

function send(io::IO, S::ServerPayload)
    println(io, "payload")
    serialize(io, S)
    return nothing
end
recieve(io::IO) = deserialize(io)

start(io::IO) = println(io, "start")
stop(io::IO) = println(io, "stop")
shutdown(io::IO) = println(io, "exit")

#####
##### Glue
#####

# Make CounterTools compatible with SystemSnoop
SystemSnoop.measure(monitor::CounterTools.CoreMonitor) = read(monitor)
SystemSnoop.measure(monitor::CounterTools.IMCMonitor) = read(monitor)
SystemSnoop.measure(monitor::CounterTools.CHAMonitor) = read(monitor)

#####
##### Set up some
#####

function runserver(port)
    # Setup a sever listening to the Named Pipe
    # Set the permissions on the server so it is readable and writable by non-sudo processes.
    println("Running Server")
    server = listen(port)
    println("SERVER UP")

    while true
        println("Waiting for connection")
        sock = accept(server)
        println("Accepted connection")
        payload = nothing

        # Wait for a "go" method
        while isopen(sock)
            cmd = readline(sock)
            println("Command: ", cmd)

            # Ping back to see if anyone's listening
            if cmd == "ping"
                println(sock, "ping")

            # Payload transmission
            elseif cmd == "payload"
                payload = deserialize(sock)

            # Start recording
            elseif cmd == "start"
                # Start sampling.
                #
                # Run Garbage collection afterwards to make sure we clean up any
                # left-over monitors.
                sample(sock, payload)
                GC.gc()

            elseif cmd == "exit"
                close(server)
                return nothing

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
    sampler = SystemSnoop.SmartSample(payload.sampletime)

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

#####
##### Method for running a function and automating talking with the server
#####

function run(f, payload::ServerPayload, port; sleeptime = nothing)
    # Connect to the server and send over the payload.
    client = Sockets.connect(port)
    ping(client)
    send(client, payload)

    # Start the counters
    start(client)

    maybesleep(sleeptime)
    runtime = @elapsed return_val = f()
    maybesleep(sleeptime)
    stop(client)

    # Get the measurement data
    data = recieve(client)
    close(client)

    return (data, return_val, runtime)
end

maybesleep(::Nothing) = nothing
maybesleep(x) = sleep(x)

end # module
