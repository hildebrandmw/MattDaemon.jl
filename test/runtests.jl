using MattDaemon
using SystemSnoop
using CounterTools

using Dates
using Sockets
using Test

@testset "MattDaemon.jl" begin
    # Test of FunctionWrapper
    fw = MattDaemon.FunctionWrapper(+, [1,2,3])
    @test MattDaemon.materialize(fw) == 1 + 2 + 3

    # Test equality
    fb = MattDaemon.FunctionWrapper(+, [1,2,3])
    @test fw == fb
    fc = MattDaemon.FunctionWrapper(+, [1,1,1])
    @test fw != fc

    # Materialize should just pass through non-FunctionWrappers
    @test MattDaemon.materialize(10) == 10

    # Make sure the macro is working
    #
    # We'll create a `SystemSnoop.Timestamp` and a `CounterTools.CoreMonitor`
    # For the CoreMonitor, we'll pretend to monitor Scalar Float64 operations.
    events = (
           CounterTools.CoreSelectRegister(event = 0xC7, umask = 0x01),
   )

    measurements = MattDaemon.@measurements (
        timestamp = SystemSnoop.Timestamp(),
        counters = CounterTools.CoreMonitor(CounterTools.IndexZero(0), events),
        dummy = 500
    )

    @test isa(measurements, NamedTuple{(:timestamp, :counters, :dummy)})

    # Now, check that this was transcoded properly.
    @test measurements.dummy == 500
    @test measurements.timestamp == MattDaemon.FunctionWrapper(SystemSnoop.Timestamp, [])

    expected = MattDaemon.FunctionWrapper(
        CounterTools.CoreMonitor,
        [CounterTools.IndexZero(0), events],
    )
    @test measurements.counters == expected

    # Now, we create some measurements WITHOUT the CoreMonitor and see that `materialize`
    # works
    #
    # This is because the CoreMonitor requires root permission to instantiate.
    measurements = MattDaemon.@measurements (
        timestamp = SystemSnoop.Timestamp(),
        dummy = 500
    )

    materialized = MattDaemon.materialize(measurements)
    @test isa(materialized, NamedTuple{(:timestamp, :dummy)})
    @test materialized.timestamp == SystemSnoop.Timestamp()
    @test materialized.dummy == 500

    # Now, we actually test that the Daemon listens properly.
    port = 2000
    local deltas
    sampling_period = Millisecond(100)

    @sync begin
        # Now we try to invoke the server
        @async begin
            measurements = MattDaemon.@measurements (
                timestamp = SystemSnoop.Timestamp(),
            )

            # Give the server some time to get setup
            sleep(1)
            client = Sockets.connect(port)

            # Ping
            @test MattDaemon.ping(client)

            # Send across the payload
            payload = MattDaemon.ServerPayload(sampling_period, measurements)
            MattDaemon.send(client, payload)
            MattDaemon.start(client)
            sleep(1)
            MattDaemon.stop(client)

            # Now, we get back the measurements
            data = MattDaemon.recieve(client)

            @test eltype(data.timestamp) == DateTime
            # Çheck that the time between measurements is about 100 milliseconds
            deltas = diff(data.timestamp)

            # Close down server
            MattDaemon.shutdown(client)
        end

        # Launch there server.
        @async begin
            MattDaemon.runserver(port)
        end
    end

    # Check the sampling times post test
    for delta in deltas
        millis = Dates.value(Millisecond(delta))
        @test millis > 90
        @test millis < 110
    end
end
