import asyncio


def test_probe_engine_runs_event_loop():
    # simple smoke test ensuring the probe loop can be started and stopped
    from auto_squid.probe_engine import ProbeEngine

    probe = ProbeEngine()

    async def runner():
        task = asyncio.create_task(probe.run_loop())
        await asyncio.sleep(0.05)
        probe.stop()
        await asyncio.wait_for(task, timeout=1.0)

    asyncio.run(runner())
