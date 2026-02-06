import os
import logging
import asyncio
import sqlite3
import hashlib
import json
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, ChatMemberHandler
)
import google.generativeai as genai
import aiohttp

# ==================== CONFIG ====================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8155847480:AAFsC7nlccy-kCEmvn3L_IIQW13YKOHSVrw")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyBzLrrKP4KVKvA1QRKtKbJVUaCLymIC2TA")
ADMIN_USERNAME = "@justrobodude"
UPDATE_INTERVAL = 30  # 30 minutes

# Configure Gemini
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-pro')

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== DATABASE ====================
class Database:
    def __init__(self):
        self.db_path = 'jobs.db'
        self.init_db()
    
    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                title TEXT,
                organization TEXT,
                qualification TEXT,
                last_date TEXT,
                apply_link TEXT,
                notification_link TEXT,
                post_date TEXT,
                location TEXT,
                hash TEXT UNIQUE,
                posted INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT,
                chat_type TEXT,
                added_by INTEGER,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def add_job(self, job):
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            hash_str = f"{job['title']}{job.get('organization', '')}{job.get('last_date', '')}"
            job_hash = hashlib.md5(hash_str.encode()).hexdigest()
            
            c.execute('''
                INSERT OR IGNORE INTO jobs 
                (source, title, organization, qualification, last_date, 
                 apply_link, notification_link, post_date, location, hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job.get('source', 'Gemini AI'),
                job.get('title'),
                job.get('organization'),
                job.get('qualification'),
                job.get('last_date'),
                job.get('apply_link'),
                job.get('notification_link'),
                job.get('post_date'),
                job.get('location', 'All India'),
                job_hash
            ))
            
            conn.commit()
            conn.close()
            return c.rowcount > 0
        except Exception as e:
            logger.error(f"DB Error: {e}")
            return False
    
    def get_unposted_jobs(self, limit=5):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT * FROM jobs WHERE posted=0 ORDER BY created_at DESC LIMIT ?', (limit,))
        columns = [description[0] for description in c.description]
        jobs = [dict(zip(columns, row)) for row in c.fetchall()]
        conn.close()
        return jobs
    
    def mark_posted(self, job_id):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('UPDATE jobs SET posted=1 WHERE id=?', (job_id,))
        conn.commit()
        conn.close()
    
    def add_channel(self, chat_id, chat_title, chat_type, added_by):
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute('''
                INSERT OR REPLACE INTO channels (chat_id, chat_title, chat_type, added_by)
                VALUES (?, ?, ?, ?)
            ''', (chat_id, chat_title, chat_type, added_by))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Add channel error: {e}")
            return False
    
    def get_active_channels(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT chat_id FROM channels WHERE is_active=1')
        channels = [row[0] for row in c.fetchall()]
        conn.close()
        return channels

db = Database()

# ==================== GEMINI AI JOB FETCHER ====================
class GeminiJobFetcher:
    def __init__(self):
        self.model = model
    
    async def fetch_jobs(self):
        """Use Gemini AI to search and format latest government jobs"""
        logger.info("=" * 60)
        logger.info("GEMINI AI JOB FETCH STARTED")
        logger.info("=" * 60)
        
        jobs = []
        
        # Search queries for different categories
        search_queries = [
            "latest government jobs in India today 2026 SSC UPSC Railway Banking",
            "sarkari naukri latest vacancies today employment news",
            "state government jobs PSC BPSC UPPSC MPPSC today",
            "defence jobs Indian Army Navy Air Force Coast Guard today",
            "teaching jobs CTET KVS NVS professor vacancies today",
            "banking jobs IBPS SBI RBI PO Clerk vacancies today",
            "railway jobs RRB NTPC Group D ALP Technician today",
            "police jobs SI Constable CAPF CRPF CISF today"
        ]
        
        for query in search_queries:
            try:
                logger.info(f"Searching: {query[:50]}...")
                
                # Create prompt for Gemini
                prompt = f"""
                Search for: {query}
                
                Find the LATEST 2-3 job notifications. For each job, provide:
                
                1. Job Title (exact post name)
                2. Organization (SSC, UPSC, Railway, etc.)
                3. Last Date to Apply (DD-MM-YYYY format)
                4. Qualification Required (10th, 12th, Graduate, etc.)
                5. Official Website Link (if available)
                6. Brief Description (2 lines max)
                
                Format as JSON:
                [
                    {{
                        "title": "...",
                        "organization": "...",
                        "last_date": "...",
                        "qualification": "...",
                        "apply_link": "...",
                        "description": "..."
                    }}
                ]
                
                Only return valid JSON. If no jobs found, return empty array [].
                """
                
                # Get response from Gemini
                response = await asyncio.to_thread(self.model.generate_content, prompt)
                text = response.text
                
                # Extract JSON from response
                json_str = self._extract_json(text)
                
                if json_str:
                    job_list = json.loads(json_str)
                    for job_data in job_list:
                        job = self._format_job(job_data, query)
                        if job:
                            jobs.append(job)
                            logger.info(f"Found: {job['title'][:50]}...")
                
                await asyncio.sleep(2)  # Rate limit
                
            except Exception as e:
                logger.error(f"Query error: {str(e)}")
                continue
        
        logger.info(f"TOTAL JOBS FROM AI: {len(jobs)}")
        logger.info("=" * 60)
        return jobs
    
    def _extract_json(self, text):
        """Extract JSON from Gemini response"""
        try:
            # Find JSON array in text
            start = text.find('[')
            end = text.rfind(']')
            if start != -1 and end != -1:
                return text[start:end+1]
            
            # Try finding JSON object
            start = text.find('{')
            end = text.rfind('}')
            if start != -1 and end != -1:
                return text[start:end+1]
            
            return None
        except:
            return None
    
    def _format_job(self, data, source_query):
        """Format AI response to job dict"""
        try:
            title = data.get('title', '').strip()
            if not title or len(title) < 10:
                return None
            
            return {
                'source': 'Gemini AI Search',
                'title': title,
                'organization': data.get('organization', 'Government of India'),
                'qualification': data.get('qualification', 'As per notification'),
                'last_date': data.get('last_date', 'Check notification'),
                'apply_link': data.get('apply_link', 'https://employmentnews.gov.in'),
                'notification_link': data.get('apply_link', 'https://employmentnews.gov.in'),
                'post_date': str(datetime.now()),
                'location': 'All India',
                'description': data.get('description', '')
            }
        except Exception as e:
            logger.error(f"Format error: {e}")
            return None

# ==================== FORMATTER ====================
def format_job(job):
    return f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🚨 GOVERNMENT JOB ALERT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📌 *{job['title']}*

🏢 Organization: {job.get('organization', 'Govt of India')}

A. 📅 IMPORTANT DATES
• Notification Date: {job.get('post_date', 'Recent')[:10]}
• Last Date to Apply: {job.get('last_date', 'Check notification')}

B. 🎓 ELIGIBILITY CRITERIA
• Educational Qualification: {job.get('qualification', 'As per notification')}
• Age Limit: Check official notification
• Nationality: Indian Citizen

C. 💰 APPLICATION DETAILS
• Application Fee: As per category (General/OBC/SC/ST)
• Payment Mode: Online/Offline

D. 📋 SELECTION PROCESS
• Written Examination
• Skill Test/Interview (if applicable)
• Document Verification

E. 📚 EXAM PATTERN
• Detailed syllabus in official notification

F. 👥 POST & SALARY DETAILS
• Posts Available: Multiple
• Pay Scale: As per 7th Pay Commission
• Location: {job.get('location', 'All India')}

G. 📄 DOCUMENTS REQUIRED
• Educational Certificates
• ID Proof (Aadhar/PAN)
• Category Certificate (if applicable)
• Passport Size Photos

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔗 IMPORTANT LINKS
• Official Notification: {job.get('notification_link', 'Check source')}
• Apply Online: {job.get('apply_link', 'Check source')}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🤖 Powered by Gemini AI
📢 Updates every 30 minutes!
⏰ Posted: {datetime.now().strftime('%d-%m-%Y %H:%M')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

def get_buttons(job):
    keyboard = [
        [InlineKeyboardButton("🚀 APPLY NOW", url=job.get('apply_link', 'https://employmentnews.gov.in'))],
        [
            InlineKeyboardButton("📋 FULL DETAILS", callback_data=f"details_{job['id']}"),
            [InlineKeyboardButton("📅 DATES", callback_data=f"dates_{job['id']}")
        ],
        [InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/justrobodude")]
    ]
    return InlineKeyboardMarkup(keyboard)

# ==================== BOT HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Welcome with add buttons"""
    user = update.effective_user
    
    welcome_text = f"""
👋 *Welcome {user.first_name}!*

🤖 *AI Powered Government Jobs Bot*

🇮🇳 I use Google Gemini AI to find latest:
• SSC, UPSC, Railway, Banking
• Defence, Police, Teaching
• All State PSCs
• 50+ Categories

⚡ *Features:*
✅ AI searches latest jobs every 30 min
✅ No website blocking
✅ Complete A-G format
✅ Works in any channel/group

📌 *Add me to your channel/group:*
• Make me admin with post permission
• I'll auto-post jobs!
"""
    
    keyboard = [
        [InlineKeyboardButton("➕ Add to Channel/Group", url=f"https://t.me/{context.bot.username}?startgroup=true")],
        [InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/justrobodude")]
    ]
    
    await update.message.reply_text(
        welcome_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """
🤖 *AI Bot Help:*

*Setup in Channel/Group:*
1. Add me as admin
2. Give "Post Messages" permission
3. Done! Auto-post every 30 min

*Commands:*
/start - Start bot
/help - This message
/update - Admin: Fetch jobs now

*Powered by Google Gemini AI*
"""
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def update_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manual fetch using AI"""
    user = update.effective_user
    chat = update.effective_chat
    
    # Check admin
    is_admin = False
    if chat.type in ['group', 'supergroup', 'channel']:
        try:
            member = await context.bot.get_chat_member(chat.id, user.id)
            if member.status in ['administrator', 'creator']:
                is_admin = True
        except:
            pass
    
    if not is_admin and user.username != ADMIN_USERNAME.replace('@', ''):
        await update.message.reply_text("❌ Admin only!")
        return
    
    await update.message.reply_text("🤖 Asking Gemini AI for latest jobs...")
    
    fetcher = GeminiJobFetcher()
    jobs = await fetcher.fetch_jobs()
    
    new_count = 0
    for job in jobs:
        if db.add_job(job):
            new_count += 1
    
    # Post to current chat
    if chat.type in ['channel', 'group', 'supergroup'] and new_count > 0:
        unposted = db.get_unposted_jobs(3)
        for job in unposted:
            try:
                await context.bot.send_message(
                    chat_id=chat.id,
                    text=format_job(job),
                    reply_markup=get_buttons(job),
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
                db.mark_posted(job['id'])
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Post error: {e}")
    
    await update.message.reply_text(
        f"✅ AI Search Complete!\n"
        f"📊 Found: {len(jobs)} jobs\n"
        f"🆕 New: {new_count} jobs\n"
        f"📢 Posted: {min(new_count, 3)}"
    )

async def chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """When bot added to channel/group"""
    result = update.my_chat_member
    
    if result.new_chat_member.status == 'administrator':
        chat = result.chat
        logger.info(f"Bot added to {chat.title} ({chat.id})")
        
        db.add_channel(chat.id, chat.title, chat.type, result.from_user.id)
        
        try:
            await context.bot.send_message(
                chat_id=chat.id,
                text=f"""
🎉 *AI Bot Activated!*

✅ I'll post government job alerts every 30 minutes using Google Gemini AI!

📌 *Commands:*
/update - Fetch jobs now
/help - Help message

🤖 Managed by @justrobodude
""",
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Welcome msg failed: {e}")

# ==================== AUTO BROADCAST ====================
async def auto_ai_fetch_and_broadcast(context: ContextTypes.DEFAULT_TYPE):
    """AI fetches and posts to all channels every 30 min"""
    logger.info("=" * 60)
    logger.info("AUTO AI BROADCAST STARTED")
    logger.info("=" * 60)
    
    fetcher = GeminiJobFetcher()
    jobs = await fetcher.fetch_jobs()
    
    new_count = 0
    for job in jobs:
        if db.add_job(job):
            new_count += 1
    
    logger.info(f"New jobs from AI: {new_count}")
    
    if new_count > 0:
        channels = db.get_active_channels()
        unposted = db.get_unposted_jobs(3)
        
        logger.info(f"Broadcasting to {len(channels)} channels")
        
        for chat_id in channels:
            for job in unposted:
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=format_job(job),
                        reply_markup=get_buttons(job),
                        parse_mode='Markdown',
                        disable_web_page_preview=True
                    )
                    await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"Failed to post to {chat_id}: {e}")
        
        for job in unposted:
            db.mark_posted(job['id'])
    
    logger.info("AUTO AI BROADCAST COMPLETED")

# ==================== MAIN ====================
def main():
    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("update", update_command))
    application.add_handler(ChatMemberHandler(chat_member_update, ChatMemberHandler.MY_CHAT_MEMBER))
    
    # Every 30 minutes
    job_queue = application.job_queue
    job_queue.run_repeating(auto_ai_fetch_and_broadcast, interval=timedelta(minutes=30), first=10)
    
    logger.info("AI Bot started! Waiting for channels...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
