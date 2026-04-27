import unittest
from types import SimpleNamespace

import primefist_bot as bot_module


class FakeBot:
    def __init__(self, updates):
        self.updates = list(updates)
        self.get_updates_calls = []
        self.sent_messages = []
        self.sent_polls = []

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


if __name__ == "__main__":
    unittest.main()
