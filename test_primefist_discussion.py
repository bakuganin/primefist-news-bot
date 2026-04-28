import unittest
from types import SimpleNamespace
from datetime import datetime, timezone

import primefist_bot as bot_module


class FakeBot:
    def __init__(self, updates, photo_error=None):
        self.updates = list(updates)
        self.photo_error = photo_error
        self.get_updates_calls = []
        self.sent_messages = []
        self.sent_polls = []
        self.sent_photos = []

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


class SourceCandidateTest(unittest.TestCase):
    def test_nitter_link_is_normalized_to_x(self):
        self.assertEqual(
            bot_module.normalize_x_link("https://nitter.net/ufc/status/12345#m"),
            "https://x.com/ufc/status/12345"
        )

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

        candidates = bot_module.extract_ufc_event_candidates(html, set(), now)

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate["id"], "ufc-event:upcoming:https://www.ufc.com/event/ufc-test")
        self.assertEqual(candidate["source"], "UFC Events")
        self.assertEqual(candidate["link"], "https://www.ufc.com/event/ufc-test")
        self.assertIn("Fighter A vs Fighter B", candidate["summary_text"])


if __name__ == "__main__":
    unittest.main()
