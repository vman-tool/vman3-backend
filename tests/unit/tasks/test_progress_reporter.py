from app.tasks.odk_tasks import ProgressReporter


class _Clock:
    """A fake now_fn whose value only changes when the test explicitly
    advances it - avoids any dependency on exactly how many times
    ProgressReporter happens to call `now_fn` internally, unlike a
    pre-baked sequence of values consumed one-by-one."""

    def __init__(self, t: float = 0.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


class TestShouldPublish:
    def test_the_very_first_call_always_publishes_even_without_force(self):
        reporter = ProgressReporter(100, 100, 0, min_interval=10, now_fn=_Clock())
        assert reporter.should_publish(force=False) is True

    def test_a_second_call_too_soon_after_the_first_is_skipped(self):
        clock = _Clock()
        reporter = ProgressReporter(100, 100, 0, min_interval=1.0, now_fn=clock)
        reporter.mark_published()
        clock.advance(0.2)  # well under the 1.0s minimum interval
        assert reporter.should_publish(force=False) is False

    def test_a_call_after_min_interval_has_elapsed_publishes(self):
        clock = _Clock()
        reporter = ProgressReporter(100, 100, 0, min_interval=1.0, now_fn=clock)
        reporter.mark_published()
        clock.advance(1.5)
        assert reporter.should_publish(force=False) is True

    def test_force_true_always_publishes_regardless_of_timing(self):
        clock = _Clock()
        reporter = ProgressReporter(100, 100, 0, min_interval=999, now_fn=clock)
        reporter.mark_published()
        # Without force this would be skipped (interval far from elapsed) -
        # force must override that.
        assert reporter.should_publish(force=False) is False
        assert reporter.should_publish(force=True) is True


class TestBuildPayload:
    def test_computes_progress_as_a_percentage_of_total(self):
        clock = _Clock(10.0)
        reporter = ProgressReporter(total_data_count=200, server_total=500, local_count=50, now_fn=clock)
        payload = reporter.build_payload(records_saved=50, start_time=0.0)

        assert payload["progress"] == 25.0
        assert payload["total_records"] == 200
        assert payload["server_total"] == 500
        assert payload["local_count"] == 50
        assert payload["records_processed"] == 50
        assert payload["elapsed_time"] == 10.0
        assert payload["status"] == "running"
        assert "50" in payload["message"] and "200" in payload["message"]

    def test_caps_progress_at_100_even_if_records_saved_exceeds_total(self):
        # Shouldn't happen in practice, but a stale/raced total must never
        # render a progress bar past full.
        reporter = ProgressReporter(total_data_count=10, server_total=10, local_count=0, now_fn=_Clock())
        payload = reporter.build_payload(records_saved=15, start_time=0.0)
        assert payload["progress"] == 100.0

    def test_zero_total_data_count_reports_zero_progress_without_dividing_by_zero(self):
        reporter = ProgressReporter(total_data_count=0, server_total=0, local_count=0, now_fn=_Clock())
        payload = reporter.build_payload(records_saved=0, start_time=0.0)
        assert payload["progress"] == 0


class TestCadenceIntegration:
    """Simulates the chunk loop's actual usage pattern: many small inserts,
    only some of which should result in an actual publish."""

    def test_only_the_first_chunk_and_forced_chunks_publish_when_chunks_arrive_faster_than_min_interval(self):
        # 5 chunks, 0.1s apart, min_interval=1.0s - only the first should
        # publish on its own; a later one only publishes when forced (as the
        # real loop forces the last chunk of each ODK page).
        clock = _Clock()
        reporter = ProgressReporter(100, 100, 0, min_interval=1.0, now_fn=clock)

        published = []
        for i, force in enumerate([False, False, False, False, True]):
            if reporter.should_publish(force=force):
                published.append(i)
                reporter.mark_published()
            clock.advance(0.1)

        assert published == [0, 4]  # first chunk, and the forced last one

    def test_chunks_spaced_further_apart_than_min_interval_all_publish(self):
        clock = _Clock()
        reporter = ProgressReporter(100, 100, 0, min_interval=0.5, now_fn=clock)

        published = []
        for i in range(4):
            if reporter.should_publish(force=False):
                published.append(i)
                reporter.mark_published()
            clock.advance(1.0)

        assert published == [0, 1, 2, 3]
