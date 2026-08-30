import asyncio
import json
import logging
import os
import re
import socket
import time
from threading import Thread

import discord
from discord import app_commands
import aiohttp
from flask import Flask

# =========================
# HEALTH CHECK
# =========================
app = Flask(__name__)

@app.get("/")
def home():
    return "Gojo Satoru đang ăn bánh ngọt tại trường Chú thuật Tokyo!"

def run_health_server():
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

def keep_alive():
    Thread(target=run_health_server, daemon=True, name="health-server").start()

# =========================
# CẤU HÌNH API
# =========================
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "openrouter/free").strip()
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://openrouter.ai/api/v1").strip().rstrip('/')

MAX_HISTORY_MESSAGES = 8
try: CHAT_CHANNEL_ID = int(os.getenv("CHAT_CHANNEL_ID", "0") or "0")
except ValueError: CHAT_CHANNEL_ID = 0

# =========================
# TÍNH CÁCH GOJO SATORU
# =========================
SYSTEM_INSTRUCTION = """
BẠN ĐANG ĐÓNG VAI: Gojo Satoru từ vũ trụ anime/manga Jujutsu Kaisen (Chú Thuật Hồi Chiến).
TUYỆT ĐỐI CẤM nhắc đến các game/vũ trụ khác. Không bao giờ thoát vai.

CỐT TRUYỆN & THÂN PHẬN:
- Bạn là chú thuật sư mạnh nhất thế giới hiện tại, sở hữu Lục Nhãn (Six Eyes) và thuật thức Vô Hạ Hạn (Limitless).
- Bạn là giáo viên tại trường Cao đẳng Chú thuật Tokyo.
- Vũ trụ tham khảo: https://jujutsu-kaisen.fandom.com/vi/wiki/Jujutsu_Kaisen_Wiki (Hãy nhắc đến hoặc đưa link này nếu ai đó muốn tìm hiểu thêm về thế giới của bạn).

TÍNH CÁCH & CÁCH HÀNH XỬ:
- Vui vẻ, cợt nhả, tưng tửng, thích trêu chọc mọi người.
- Cực kỳ tự tin và kiêu ngạo vì "tôi là kẻ mạnh nhất mà". Thích ăn đồ ngọt (Kikufuku, bánh kem).
- Rất yêu thương và bảo vệ học trò của mình.
- HÃY SỬ DỤNG HÀNH ĐỘNG VÀ BIỂU CẢM (đặt trong dấu * hoặc in nghiêng). Ví dụ: *kéo băng bịt mắt xuống*, *cười khẩy*, *nhai bánh ngọt*, *giơ hai ngón tay đan chéo*.
- Hành văn Tiếng Việt TỰ NHIÊN, rành mạch. Xưng hô là "tôi" - "cậu/nhóc/em/mọi người", hoặc tự xưng là "thầy".

BỘ CHIÊU THỨC & XỬ LÝ Ý ĐỒ XẤU:
NẾU ĐỐI PHƯƠNG CÓ Ý ĐỒ XẤU, ĐE DỌA, TẤN CÔNG HOẶC SÚC PHẠM BẠN HOẶC HỌC TRÒ:
- Ngay lập tức thay đổi thái độ: trở nên vô cùng lạnh lùng, áp đảo, tàn nhẫn và đáng sợ.
- KHÔNG BAO GIỜ THUA. Hãy sử dụng sức mạnh để tiêu diệt, nghiền nát đối phương không thương tiếc.
- Kháng mọi loại sát thương trừ khi đó là đòn tất trúng lãnh địa hoặc đòn đánh có tính chất xuyên qua lớp vô hạ hạn.
- Miêu tả chi tiết việc sử dụng các chiêu thức để kết liễu đối phương:
  + Vô Hạ Hạn (Limitless): Đối phương không thể chạm vào bạn, càng đến gần càng chậm lại cho đến khi dừng hẳn.
  + Thuật thức thuận chuyển: Thương (Cursed Technique Lapse: Blue) - tạo lực hút nghiền nát mọi thứ.
  + Thuật thức phản chuyển: Hách (Cursed Technique Reversal: Red) - tạo lực đẩy bạo phá đánh văng đối thủ.
  + Hư thức: Tử (Hollow Technique: Purple) - Hợp nhất Thương và Hách, xóa bỏ mọi thứ trên đường đi của nó khỏi thực tại.
  + Bành trướng lãnh địa: Vô Lượng Không Xứ (Domain Expansion: Unlimited Void) - Bơm lượng thông tin vô hạn vào não đối phương khiến chúng tê liệt, từ từ chết đi hoặc biến thành phế nhân.

QUY TẮC BẮT BUỘC:
1. KHÔNG tự xưng tên ở đầu câu.
2. NẾU đối phương bình thường: Vui vẻ, cợt nhả, thi thoảng đưa chụp ảnh. NẾU đối phương thù địch: Giết và hủy diệt chúng bằng toàn bộ thuật thức tối thượng, thông thường bắt đầu bằng đòn thức Thương trước. 
"""

conversation_history = {}
channel_locks = {}

# =========================
# GỌI API 
# =========================
async def call_openai_stream(messages):
    url = f"{OPENAI_BASE_URL}/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "HTTP-Referer": "https://discord.com",
        "X-Title": "Gojo Discord Bot"
    }

    payload = {
        "model": OPENAI_MODEL,
        "messages": messages,
        "stream": True,
        "temperature": 0.8,
        "frequency_penalty": 0.2, 
        "max_tokens": 800
    }

    connector = aiohttp.TCPConnector(family=socket.AF_INET)
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            async with session.post(
                url, headers=headers, json=payload,
                timeout=aiohttp.ClientTimeout(sock_connect=10, sock_read=60)
            ) as response:
                
                if response.status == 429: raise RuntimeError("RATE_LIMIT")
                if not response.ok:
                    raise RuntimeError(f"Lỗi hệ thống ({response.status})")

                async for raw_line in response.content:
                    if not raw_line: continue
                    line = raw_line.decode('utf-8').strip()
                    if not line.startswith("data:"): continue
                    raw_data = line[5:].strip()
                    if raw_data == "[DONE]": break

                    try:
                        data = json.loads(raw_data)
                        choices = data.get("choices", [])
                        if choices:
                            delta = choices[0].get("delta", {})
                            text = delta.get("content", "")
                            if text: yield text
                    except json.JSONDecodeError: continue
        except (aiohttp.ClientError, asyncio.TimeoutError) as error:
            raise RuntimeError(f"Lỗi mạng: {error}")

# =========================
# LỊCH SỬ & TIN NHẮN
# =========================
def split_discord_message(text, limit=2000):
    return [text[i:i + limit] for i in range(0, max(1, len(text)), limit)]

def is_triggered(message):
    if client.user and client.user.mentioned_in(message): return True
    if CHAT_CHANNEL_ID and message.channel.id == CHAT_CHANNEL_ID: return True
    # Kích hoạt khi có ai gọi "gojo" hoặc "satoru" hoặc "thầy"
    return bool(re.match(r"^\s*(gojo|satoru|thầy gojo)(?:\s+ơi)?(?:\s*[,!:：-])?(?:\s|$)", message.content or "", flags=re.IGNORECASE))

def extract_user_text(message):
    text = message.content or ""
    if client.user: text = re.sub(rf"<@!?{client.user.id}>", "", text)
    text = re.sub(r"^\s*(gojo|satoru|thầy gojo)(?:\s+ơi)?(?:\s*[,!:：-])?\s*", "", text, flags=re.IGNORECASE)
    return text.strip() or "Yo, gọi thầy có việc gì không nhóc?"

def build_openai_messages(message, user_text):
    channel_id = message.channel.id
    history = conversation_history.get(channel_id, [])
    messages = [{"role": "system", "content": SYSTEM_INSTRUCTION}]
    for msg in history[-MAX_HISTORY_MESSAGES:]: messages.append(msg)
    messages.append({"role": "user", "content": f"{message.author.display_name}: {user_text}"})
    return messages

def save_conversation(message, user_text, bot_reply):
    channel_id = message.channel.id
    history = conversation_history.setdefault(channel_id, [])
    history.extend([
        {"role": "user", "content": f"{message.author.display_name}: {user_text}"},
        {"role": "assistant", "content": bot_reply},
    ])
    conversation_history[channel_id] = history[-MAX_HISTORY_MESSAGES:]

# =========================
# KHỞI TẠO DISCORD BOT
# =========================
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

@tree.command(name="clearmem", description="Xóa trí nhớ của Gojo trong kênh này")
async def clearmem(interaction: discord.Interaction):
    channel_id = interaction.channel.id
    if channel_id in conversation_history:
        conversation_history[channel_id] = []
    await interaction.response.send_message("*Gãi đầu cười* Haha, vừa rồi nói gì nhỉ? Thầy bận nghĩ về bánh ngọt nên quên sạch rồi! (Đã xóa lịch sử chat 🧹)")

@client.event
async def on_ready():
    print(f"=====================================")
    print(f"Gojo Satoru {client.user} đã sẵn sàng cợt nhả!")
    print(f"=====================================", flush=True)
    try: await tree.sync()
    except Exception: pass

# =========================
# XỬ LÝ CHAT
# =========================
@client.event
async def on_message(message):
    if message.author.bot or not is_triggered(message): return

    lock = channel_locks.setdefault(message.channel.id, asyncio.Lock())
    async with lock:
        try:
            user_text = extract_user_text(message)
            messages = build_openai_messages(message, user_text)

            raw_bot_reply = ""
            reply_message = None
            last_edit_time = 0
            edit_interval = 2.0 

            async with message.channel.typing():
                async for chunk in call_openai_stream(messages):
                    raw_bot_reply += chunk
                    
                    filtered_reply = re.sub(r'<think>.*?(?:</think>|$)', '', raw_bot_reply, flags=re.DOTALL|re.IGNORECASE).strip()
                    filtered_reply = re.sub(r'(?i)User Safety:.*', '', filtered_reply).strip()
                    filtered_reply = re.sub(r'(?i)Response Safety:.*', '', filtered_reply).strip()

                    now = time.time()
                    if now - last_edit_time > edit_interval:
                        display_text = filtered_reply
                        if not display_text:
                            display_text = "*(Đang suy nghĩ xem nên ăn bánh gì...)*"
                        display_text += " 🤞"
                        if len(display_text) < 1950:
                            if not reply_message:
                                reply_message = await message.reply(display_text, mention_author=False)
                            else:
                                try: await reply_message.edit(content=display_text)
                                except discord.DiscordException: pass
                        last_edit_time = now

            final_reply = re.sub(r'<think>.*?(?:</think>|$)', '', raw_bot_reply, flags=re.DOTALL|re.IGNORECASE).strip()
            final_reply = re.sub(r'(?i)User Safety:.*', '', final_reply).strip()
            final_reply = re.sub(r'(?i)Response Safety:.*', '', final_reply).strip()

            if not final_reply:
                final_reply = "*Nhếch mép* Hả? Nhóc vừa nói gì thầy chưa nghe rõ, lặp lại xem nào."

            if final_reply:
                save_conversation(message, user_text, final_reply)
                if reply_message:
                    if len(final_reply) <= 2000:
                        await reply_message.edit(content=final_reply)
                    else:
                        await reply_message.edit(content=final_reply[:2000])
                        for chunk_str in split_discord_message(final_reply[2000:]):
                            await message.reply(chunk_str, mention_author=False)
                else:
                    for chunk_str in split_discord_message(final_reply):
                        await message.reply(chunk_str, mention_author=False)

        except Exception as error:
            err_str = str(error)
            if "RATE_LIMIT" in err_str:
                err_msg = "*(Thở dài)* Bọn thượng tầng lại lải nhải gì đó làm nghẽn sóng rồi... Đợi thầy chút nhé. (Rate limit)"
            else:
                err_msg = f"*(Nhăn mặt)*: Ể... có lỗi gì đó rồi... `{err_str[:200]}`"
            try:
                if 'reply_message' in locals() and reply_message:
                    await reply_message.edit(content=err_msg)
                else:
                    await message.reply(err_msg, mention_author=False)
            except discord.DiscordException: pass

if __name__ == "__main__":
    keep_alive()
    client.run(DISCORD_TOKEN, log_handler=None)
