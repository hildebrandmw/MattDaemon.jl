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
FunctionWrapper(f, args, ::Nothing) = FunctionWrapper(f, args, NamedTuple())
Base.:(==)(a::FunctionWrapper, b::FunctionWrapper) = (a.f == b.f) && (a.args == b.args) && (a.kw == b.kw)

function wrapfunction(ex)
    # Keyword only functions
    if MacroTools.@capture(ex, f_(; kw__) | f_(args__; kw__) | f_(args__))
        f = esc(f)
        args = isnothing(args) ? () : esc.(args)
        kw = isnothing(kw) ? NamedTuple() : esc.(kw)
        return :(FunctionWrapper($f, [$(args...)], (;$(kw...))))
    end
    return esc(ex)
end

macro measurement(ex)
    return wrapfunction(ex)
end

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
    # Wrap any function calls into "Payloads"s
    values = map(x -> wrapfunction(x.args[2]), ex.args)

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
#####
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

            # Who knows what happened.
            else
                println("Unknown Command: $cmd")
            end
        end
    end
end

sample(sock, ::Nothing) = nothing
function sample(sock, payload)
    nt = materialize(payload.measurements)
    sampletime = payload.sampletime

    trace = @snooped nt sampletime begin
        while true
            ln = readline(sock)
            if ln == "stop"
                break
            else
                println("Unhandled command: $ln")
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
