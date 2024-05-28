import asyncio
from aiogram import Bot, Dispatcher, F, types

from config import TOKEN
from utils import create_response

bot = Bot(token=TOKEN)
dispatcher = Dispatcher()


@dispatcher.message(F.text)
async def handle_message(message: types.Message):
    response = await create_response(message.text)
    await message.answer(response)


async def start_bot():
    await dispatcher.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(start_bot())
