import os
import logging
import asyncio
import sqlite3
import hashlib
import feedparser
import aiohttp
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, JobQueue
)

# ==================== CONFIG ====================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8155847480:AAFsC7nlccy-kCEmvn3L_IIQW13YKOHSVrw")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1003632128683"))
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "@Roboallbotchannel")
ADMIN_ID = int(os.getenv("ADMIN_ID", "6593860853"))

# Official Government Sources
SOURCES = {
    'employment_news': 'https://employmentnews.gov.in/rss-feed',
    'ssc': 'https://ssc.nic.in/rss-feed',
    'upsc': 'https://upsc.gov.in/rss-feed',
    'tnpsc': 'https://tnpsc.gov.in/rss-feed',
    'uppsc': 'https://uppsc.up.nic.in/rss'
}

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
        
        # Jobs table
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
        
        # Users table
        c.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                is_verified INTEGER DEFAULT 0,
                joined_at TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def add_job(self, job):
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            # Create unique hash
            hash_str = f"{job['title']}{job.get('organization', '')}"
            job_hash = hashlib.md5(hash_str.encode()).hexdigest()
            
            c.execute('''
                INSERT OR IGNORE INTO jobs 
                (source, title, organization, qualification, last_date, 
                 apply_link, notification_link, post_date, location, hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job.get('source'),
                job.get('title'),
                job.get('organization'),
                job.get('qualification'),
                job.get('last_date'),
                job.get('apply_link'),
                job.get('notification_link'),
                job.get('post_date'),
                job.get('location'),
                job_hash
            ))
            
            conn.commit()
            conn.close()
            return True
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

db = Database()

# ==================== JOB FETCHER ====================
async def fetch_rss_jobs(url, source_name):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=30) as response:
                content = await response.text()
                feed = feedparser.parse(content)
                jobs = []
                
                for entry in feed.entries:
                    title = entry.get('title', '')
                    summary = entry.get('summary', '')
                    
                    # Extract organization from title
                    org = 'Government of India'
                    for o in ['SSC', 'UPSC', 'RRB', 'IBPS', 'NVS', 'ESIC', 'BECIL', 'SAIL']:
                        if o in title.upper():
                            org = o
                            break
                    
                    # Extract last date
                    last_date = 'Check notification'
                    import re
                    date_match = re.search(r'(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})', summary)
                    if date_match:
                        last_date = date_match.group(1)
                    
                    job = {
                        'source': source_name,
                        'title': title,
                        'organization': org,
                        'qualification': 'As per notification',
                        'last_date': last_date,
                        'apply_link': entry.get('link', ''),
                        'notification_link': entry.get('link', ''),
                        'post_date': entry.get('published', ''),
                        'location': 'All India'
                    }
                    jobs.append(job)
                
                return jobs
    except Exception as e:
        logger.error(f"Error fetching {source_name}: {e}")
        return []

# ==================== MESSAGE FORMATTER ====================
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
📢 Join: {CHANNEL_USERNAME}
🏷️ Source: {job.get('source', 'Official')}
⏰ Posted: {datetime.now().strftime('%d-%m-%Y %H:%M')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

def get_buttons(job):
    keyboard = [
        [InlineKeyboardButton("🚀 APPLY NOW", url=job.get('apply_link', 'https://employmentnews.gov.in'))],
        [
            InlineKeyboardButton("📋 DETAILS", callback_data=f"details_{job['id']}"),
            InlineKeyboardButton("📅 DATES", callback_data=f"dates_{job['id']}")
        ],
        [InlineKeyboardButton("🤖 BOT HELP", callback_data="help")]
    ]
    return InlineKeyboardMarkup(keyboard)

# ==================== BOT HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    # Check channel membership
    try:
        member = await context.bot.get_chat_member(CHANNEL_ID, user.id)
        if member.status not in ['member', 'administrator', 'creator']:
            raise Exception("Not a member")
    except:
        keyboard = [[InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{CHANNEL_USERNAME[1:]}")],
                   [InlineKeyboardButton("✅ Verify", callback_data="verify")]]
        await update.message.reply_text(
            "⚠️ *Please join our channel first to use this bot!*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return
    
    # Save user
    conn = sqlite3.connect('jobs.db')
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO users (user_id, username, first_name, is_verified, joined_at) VALUES (?, ?, ?, 1, ?)',
              (user.id, user.username, user.first_name, datetime.now()))
    conn.commit()
    conn.close()
    
    await update.message.reply_text(f"""
👋 Welcome *{user.first_name}*!

✅ You are verified!

🚀 Use /latest to see recent jobs
🔍 Use /search [keyword] to find jobs
❓ Use /help for all commands

Stay updated with genuine government jobs!
""", parse_mode='Markdown')

async def verify_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    try:
        member = await context.bot.get_chat_member(CHANNEL_ID, user_id)
        if member.status in ['member', 'administrator', 'creator']:
            await query.edit_message_text("✅ *Verified!* Use /latest to see jobs", parse_mode='Markdown')
        else:
            await query.edit_message_text("❌ *Not joined yet!* Join the channel first.", parse_mode='Markdown')
    except Exception as e:
        await query.edit_message_text("❌ Error verifying. Try again.")

async def latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    # Verify membership
    try:
        member = await context.bot.get_chat_member(CHANNEL_ID, user.id)
        if member.status not in ['member', 'administrator', 'creator']:
            await start(update, context)
            return
    except:
        await start(update, context)
        return
    
    jobs = db.get_unposted_jobs(5)
    
    if not jobs:
        await update.message.reply_text("🔄 No new jobs right now. Check back later!")
        return
    
    for job in jobs:
        try:
            await update.message.reply_text(
                format_job(job),
                reply_markup=get_buttons(job),
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.error(f"Error sending job: {e}")

async def search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /search SSC\nUsage: /search Railway")
        return
    
    keyword = ' '.join(context.args).upper()
    conn = sqlite3.connect('jobs.db')
    c = conn.cursor()
    c.execute("SELECT * FROM jobs WHERE UPPER(title) LIKE ? LIMIT 10", (f'%{keyword}%',))
    columns = [description[0] for description in c.description]
    jobs = [dict(zip(columns, row)) for row in c.fetchall()]
    conn.close()
    
    if not jobs:
        await update.message.reply_text(f"❌ No jobs found for '{keyword}'")
        return
    
    for job in jobs:
        await update.message.reply_text(format_job(job), parse_mode='Markdown')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("""
🤖 *Commands List*

/start - Start bot & verify
/latest - Latest 5 jobs
/search [keyword] - Search jobs
/help - This message

📢 Channel: @Roboallbotchannel
""", parse_mode='Markdown')

# ==================== SCHEDULED TASKS ====================
async def fetch_and_post(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Fetching new jobs...")
    
    all_jobs = []
    
    # Fetch from all sources
    for name, url in SOURCES.items():
        jobs = await fetch_rss_jobs(url, name)
        all_jobs.extend(jobs)
        await asyncio.sleep(1)  # Be polite
    
    # Add to database
    new_count = 0
    for job in all_jobs:
        if db.add_job(job):
            new_count += 1
    
    logger.info(f"Added {new_count} new jobs")
    
    # Post to channel
    if new_count > 0:
        jobs = db.get_unposted_jobs(3)
        for job in jobs:
            try:
                await context.bot.send_message(
                    chat_id=CHANNEL_ID,
                    text=format_job(job),
                    reply_markup=get_buttons(job),
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
                db.mark_posted(job['id'])
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Post error: {e}")

# ==================== MAIN ====================
def main():
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("latest", latest))
    application.add_handler(CommandHandler("search", search))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CallbackQueryHandler(verify_callback, pattern="^verify$"))
    
    # Scheduled job every 3 hours
    job_queue = application.job_queue
    job_queue.run_repeating(fetch_and_post, interval=timedelta(hours=3), first=10)
    
    logger.info("Bot started!")
    application.run_polling()

if __name__ == "__main__":
    main()
