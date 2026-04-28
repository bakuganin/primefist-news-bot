import unittest
import tempfile
from types import SimpleNamespace
from datetime import datetime, timezone
from unittest.mock import patch

import primefist_bot as bot_module


class FakeBot:
    def __init__(self, updates, photo_error=None, video_error=None):
        self.updates = list(updates)
        self.photo_error = photo_error
        self.video_error = video_error
        self.get_updates_calls = []
        self.sent_messages = []
        self.sent_polls = []
        self.sent_photos = []
        self.sent_videos = []

    async def get_updates(self, **kwargs):
        self.get_updates_calls.append(kwargs)
        if self.updates:
            return self.updates.pop(0)
        return []

    async def send_message(self, **kwargs):
        self.sent_messages.append(kwargs)
        return SimpleNamespace(message_id=999)

    async def send_poll(self, **kwargs):
        self.sent_polls.append(kwargs)
        return SimpleNamespace(message_id=1000)

    async def send_photo(self, **kwargs):
        self.sent_photos.append(kwargs)
        if self.photo_error:
            raise self.photo_error
        return SimpleNamespace(message_id=1001)

    async def send_video(self, **kwargs):
        self.sent_videos.append(kwargs)
        if self.video_error:
            raise self.video_error
        return SimpleNamespace(message_id=1002)


class DiscussionCommentTest(unittest.IsolatedAsyncioTestCase):
    async def test_continuation_is_sent_as_reply_in_discussion_chat(self):
        fake_bot = FakeBot([
            [{
                "update_id": 1001,
                "message": {
                    "message_id": 888,
                    "chat": {"id": -1003036612049},
                    "sender_chat": {"id": -1003902344210, "username": "primefist"},
                    "is_automatic_forward": True,
                    "forward_origin": {
                        "type": "channel",
                        "chat": {"id": -1003902344210, "username": "primefist"},
                        "message_id": 777
                    }
                }
            }]
        ])

        sent_post = SimpleNamespace(message_id=777)
        sent_count = await bot_module.send_continuation_comment(
            fake_bot,
            "-1003902344210",
            sent_post,
            "Full story",
            -1003036612049,
            update_offset=1001
        )

        self.assertEqual(sent_count, 1)
        self.assertEqual(len(fake_bot.sent_messages), 1)
        sent_message = fake_bot.sent_messages[0]
        self.assertEqual(sent_message["chat_id"], -1003036612049)
        self.assertEqual(sent_message["text"], "Full story")
        self.assertNotEqual(sent_message["chat_id"], "-1003902344210")
        self.assertEqual(sent_message["reply_parameters"].message_id, 888)
        self.assertFalse(sent_message["reply_parameters"].allow_sending_without_reply)

    async def test_continuation_is_skipped_when_forward_is_not_visible(self):
        previous_wait = bot_module.DISCUSSION_FORWARD_WAIT_SECONDS
        previous_poll = bot_module.DISCUSSION_FORWARD_POLL_SECONDS
        bot_module.DISCUSSION_FORWARD_WAIT_SECONDS = 0
        bot_module.DISCUSSION_FORWARD_POLL_SECONDS = 0
        try:
            fake_bot = FakeBot([[]])
            sent_post = SimpleNamespace(message_id=19)

            sent_count = await bot_module.send_continuation_comment(
                fake_bot,
                "-1003902344210",
                sent_post,
                "Full story",
                -1003988055581,
                update_offset=2000
            )
        finally:
            bot_module.DISCUSSION_FORWARD_WAIT_SECONDS = previous_wait
            bot_module.DISCUSSION_FORWARD_POLL_SECONDS = previous_poll

        self.assertEqual(sent_count, 0)
        self.assertEqual(fake_bot.sent_messages, [])

    async def test_poll_is_sent_as_reply_in_discussion_chat(self):
        fake_bot = FakeBot([])

        await bot_module.send_discussion_poll(
            fake_bot,
            -1003036612049,
            888,
            "Who wins?",
            ["A", "B", "C", "D", "E"]
        )

        self.assertEqual(len(fake_bot.sent_polls), 1)
        sent_poll = fake_bot.sent_polls[0]
        self.assertEqual(sent_poll["chat_id"], -1003036612049)
        self.assertEqual(sent_poll["question"], "Who wins?")
        self.assertEqual(sent_poll["options"], ["A", "B", "C", "D"])
        self.assertEqual(sent_poll["reply_parameters"].message_id, 888)
        self.assertFalse(sent_poll["reply_parameters"].allow_sending_without_reply)

    async def test_channel_post_falls_back_to_text_when_photo_fails(self):
        fake_bot = FakeBot([], photo_error=RuntimeError("bad image"))

        sent = await bot_module.send_channel_post(
            fake_bot,
            "-1003902344210",
            "https://example.com/image.jpg",
            "<b>Post</b>"
        )

        self.assertEqual(sent.message_id, 999)
        self.assertEqual(len(fake_bot.sent_photos), 1)
        self.assertEqual(len(fake_bot.sent_messages), 1)
        self.assertEqual(fake_bot.sent_messages[0]["text"], "<b>Post</b>")

    async def test_channel_post_prefers_video_over_photo(self):
        fake_bot = FakeBot([])
        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = f"{tmpdir}/video.mp4"
            with open(video_path, "wb") as video:
                video.write(b"video")

            with patch.object(bot_module, "download_video_for_telegram", return_value=video_path):
                sent = await bot_module.send_channel_post(
                    fake_bot,
                    "-1003902344210",
                    "https://example.com/image.jpg",
                    "<b>Post</b>",
                    "https://x.com/ufc/status/12345"
                )

        self.assertEqual(sent.message_id, 1002)
        self.assertEqual(len(fake_bot.sent_videos), 1)
        self.assertEqual(fake_bot.sent_videos[0]["video"].name, video_path)
        self.assertEqual(fake_bot.sent_photos, [])
        self.assertEqual(fake_bot.sent_messages, [])


class SourceCandidateTest(unittest.TestCase):
    def test_nitter_link_is_normalized_to_x(self):
        self.assertEqual(
            bot_module.normalize_x_link("https://nitter.net/ufc/status/12345#m"),
            "https://x.com/ufc/status/12345"
        )

    def test_fallback_primefist_text_has_required_fields(self):
        data = bot_module.fallback_primefist_text(
            "UFC X: Big update",
            "A fresh update from the official UFC account.",
            "en"
        )

        for key in ("hook_ru", "hook_en", "short_ru", "short_en", "full_ru", "full_en"):
            self.assertTrue(data[key])
        self.assertEqual(data["poll_question"], "")
        self.assertEqual(data["poll_options"], [])

    def test_fallback_x_text_uses_specific_story_not_generic_link_copy(self):
        data = bot_module.fallback_primefist_text(
            'UFC X: "Perfect shot, perfection combination." @ErcegSteve is preparing for a hometown #UFCPerth win this weekend!',
            '"Perfect shot, perfection combination." @ErcegSteve is preparing for a hometown #UFCPerth win this weekend! [ May 2 at 7amET | LIVE on @ParamountPlus ] Video',
            "en"
        )

        self.assertIn("Эрцег", data["hook_ru"])
        self.assertIn("Erceg", data["hook_en"])
        self.assertIn("Стив Эрцег", data["short_ru"])
        self.assertIn("Steve Erceg", data["short_en"])
        self.assertNotIn("Откройте оригинальный", data["full_ru"])

    def test_ufc_event_candidate_is_extracted_from_card_html(self):
        html = """
        <article class="c-card-event--result">
          <h3 class="c-card-event--result__headline">
            <a href="/event/ufc-test">Fighter A vs Fighter B</a>
          </h3>
          <div class="c-card-event--result__date">Sat, May 2 / 2:00 PM EEST / Main Card</div>
          <div class="c-card-event--result__location">Test Arena Tallinn Estonia</div>
          <img src="/images/test.jpg" />
        </article>
        """
        now = datetime(2026, 4, 28, tzinfo=timezone.utc)

        with patch.object(
            bot_module,
            "extract_ufc_fight_summaries",
            return_value=["Fighter A vs Fighter B (Welterweight Bout)"],
        ):
            candidates = bot_module.extract_ufc_event_candidates(html, set(), now)

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate["id"], "ufc-event:upcoming:https://www.ufc.com/event/ufc-test")
        self.assertEqual(candidate["source"], "UFC Events")
        self.assertEqual(candidate["link"], "https://www.ufc.com/event/ufc-test")
        self.assertIn("Fighter A vs Fighter B", candidate["summary_text"])

    def test_completed_ufc_fight_summary_includes_winner_and_method(self):
        fight = {
            "red_name": "Aljamain Sterling",
            "blue_name": "Youssef Zalal",
            "red_outcome": "Win",
            "blue_outcome": "Loss",
            "method": "Decision - Unanimous",
            "round": "5",
            "weight_class": "Featherweight Bout",
        }

        summary = bot_module.format_ufc_fight_summary(fight, "completed")

        self.assertEqual(summary, "Aljamain Sterling def. Youssef Zalal (Decision - Unanimous, R5)")

    def test_upcoming_ufc_fight_summary_includes_weight_class(self):
        fight = {
            "red_name": "Jack Della Maddalena",
            "blue_name": "Carlos Prates",
            "red_outcome": "",
            "blue_outcome": "",
            "method": "",
            "round": "",
            "weight_class": "Welterweight Bout",
        }

        summary = bot_module.format_ufc_fight_summary(fight, "upcoming")

        self.assertEqual(summary, "Jack Della Maddalena vs Carlos Prates (Welterweight Bout)")


if __name__ == "__main__":
    unittest.main()
