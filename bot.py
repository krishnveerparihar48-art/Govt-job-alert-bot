import os
import logging
import asyncio
import sqlite3
import hashlib
import feedparser
import aiohttp
import re
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, ChatMemberHandler
)

# ==================== CONFIG ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USERNAME = "@justrobodude"
UPDATE_INTERVAL = 30  # 30 minutes

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
        
        # Channels/Groups table
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

# ==================== SCRAPER (RSS + HTML) ====================
class JobScraper:
    def __init__(self):
        self.session = None
        # RSS Sources
        self.rss_sources = {
            'employment_news': 'https://employmentnews.gov.in/rss-feed',
            'ssc': 'https://ssc.nic.in/rss-feed',
            'upsc': 'https://upsc.gov.in/rss-feed',
            'tnpsc': 'https://tnpsc.gov.in/rss-feed',
            'uppsc': 'https://uppsc.up.nic.in/rss',
            'ibps': 'https://ibps.in/rss-feed',
            'bpsc': 'https://bpsc.bih.nic.in/rss',
            'mppsc': 'https://mppsc.nic.in/rss',
            'rpsc': 'https://rpsc.rajasthan.gov.in/rss',
            'mpsc': 'https://mpsc.gov.in/rss',
            'gpsc': 'https://gpsc.gujarat.gov.in/rss',
            'cgpsc': 'https://cgpsc.gov.in/rss',
            'opsc': 'https://opsc.gov.in/rss',
            'appsc': 'https://appsc.gov.in/rss',
            'tspsc': 'https://tspsc.gov.in/rss',
            'kpsc': 'https://kpsc.kar.nic.in/rss',
            'wbpsc': 'https://wbpsc.gov.in/rss',
            'hpsc': 'https://hpsc.gov.in/rss',
            'ppsc': 'https://ppsc.gov.in/rss',
            'hppsc': 'https://hppsc.hp.gov.in/rss',
            'ukpsc': 'https://ukpsc.gov.in/rss',
            'jpsc': 'https://jpsc.gov.in/rss',
            'nvs': 'https://navodaya.gov.in/rss',
            'kvs': 'https://kvsangathan.nic.in/rss',
            'esic': 'https://esic.nic.in/rss',
            'sail': 'https://sail.co.in/rss',
            'bhel': 'https://bhel.com/rss',
            'ntpc': 'https://ntpc.co.in/rss',
            'ctet': 'https://ctet.nic.in/rss',
            'nta': 'https://nta.ac.in/rss',
            'aiims': 'https://aiimsexams.ac.in/rss',
            'rrb': 'https://rrbcdg.gov.in/rss',
            'rrc': 'https://rrcb.gov.in/rss',
            'indian_army': 'https://joinindianarmy.nic.in/rss',
            'indian_navy': 'https://joinindiannavy.gov.in/rss',
            'air_force': 'https://careerindianairforce.cdac.in/rss',
            'coast_guard': 'https://joinindiancoastguard.gov.in/rss',
            'crpf': 'https://crpf.gov.in/rss',
            'cisf': 'https://cisf.gov.in/rss',
            'itbp': 'https://itbp.gov.in/rss',
            'ssb': 'https://ssb.nic.in/rss',
            'delhi_police': 'https://delhipolice.nic.in/rss',
            'becil': 'https://becil.com/rss',
            'ongc': 'https://ongcindia.com/rss'
        }
        
        # HTML Sources (for scraping if RSS fails)
        self.html_sources = {
            'employment_news_html': 'https://employmentnews.gov.in/latest-jobs',
            'ssc_html': 'https://ssc.nic.in/portal/latest-news',
            'upsc_html': 'https://upsc.gov.in/examinations/active-examinations'
        }
    
    async def init_session(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        self.session = aiohttp.ClientSession(headers=headers)
        logger.info("Session initialized")
    
    async def close_session(self):
        if self.session:
            await self.session.close()
            logger.info("Session closed")
    
    async def fetch_rss(self, url, source_name):
        try:
            logger.info(f"[RSS] Fetching {source_name}...")
            async with self.session.get(url, timeout=30, ssl=False) as response:
                if response.status != 200:
                    logger.warning(f"[RSS] {source_name}: HTTP {response.status}")
                    return []
                
                content = await response.text()
                feed = feedparser.parse(content)
                
                jobs = []
                for entry in feed.entries[:3]:  # Top 3 only
                    job = self._parse_entry(entry, source_name)
                    if job:
                        jobs.append(job)
                
                logger.info(f"[RSS] {source_name}: {len(jobs)} jobs")
                return jobs
                
        except Exception as e:
            logger.error(f"[RSS] {source_name} error: {str(e)}")
            return []
    
    async def fetch_html(self, url, source_name):
        """Fallback HTML scraping"""
        try:
            logger.info(f"[HTML] Fetching {source_name}...")
            async with self.session.get(url, timeout=30, ssl=False) as response:
                if response.status != 200:
                    return []
                
                html = await response.text()
                jobs = self._parse_html(html, source_name, url)
                logger.info(f"[HTML] {source_name}: {len(jobs)} jobs")
                return jobs
                
        except Exception as e:
            logger.error(f"[HTML] {source_name} error: {str(e)}")
            return []
    
    def _parse_html(self, html, source, url):
        """Parse HTML for job listings"""
        from bs4 import BeautifulSoup
        jobs = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Common patterns for job listings
            selectors = [
                'table tr', '.job-listing', '.vacancy', '.notification',
                'article', '.news-item', '.list-group-item'
            ]
            
            for selector in selectors:
                items = soup.select(selector)[:3]
                for item in items:
                    text = item.get_text(strip=True)
                    if len(text) > 20 and any(keyword in text.lower() for keyword in ['recruitment', 'vacancy', 'post', 'job', 'notification']):
                        job = {
                            'source': f"{source}_html",
                            'title': text[:100] + '...' if len(text) > 100 else text,
                            'organization': self._extract_org(text),
                            'qualification': self._extract_qualification(text),
                            'last_date': self._extract_date(text),
                            'apply_link': url,
                            'notification_link': url,
                            'post_date': str(datetime.now()),
                            'location': 'All India'
                        }
                        jobs.append(job)
                        break  # Only take first valid from each selector
                if jobs:
                    break
                    
        except Exception as e:
            logger.error(f"HTML parse error: {e}")
        
        return jobs
    
    def _parse_entry(self, entry, source):
        title = entry.get('title', '').strip()
        summary = entry.get('summary', '').strip()
        link = entry.get('link', '').strip()
        
        if not title or len(title) < 10:
            return None
        
        return {
            'source': source,
            'title': title,
            'organization': self._extract_org(title),
            'qualification': self._extract_qualification(summary),
            'last_date': self._extract_date(summary) or self._extract_date(title),
            'apply_link': link,
            'notification_link': link,
            'post_date': entry.get('published', str(datetime.now())),
            'location': 'All India'
        }
    
    def _extract_org(self, text):
        orgs = ['SSC', 'UPSC', 'RRB', 'RRC', 'IBPS', 'NVS', 'KVS', 'ESIC', 'SAIL', 'BHEL', 'NTPC', 'ONGC', 'BECIL',
                'UPPSC', 'BPSC', 'MPPSC', 'TNPSC', 'RPSC', 'MPSC', 'GPSC', 'CGPSC', 'OPSC', 'APPSC', 'TSPSC', 'KPSC',
                'WBPSC', 'HPSC', 'PPSC', 'HPPSC', 'UKPSC', 'JPSC', 'CTET', 'AIIMS', 'NVS', 'KVS', 'CRPF', 'CISF', 'ITBP', 'SSB']
        for org in orgs:
            if org in text.upper():
                return org
        return 'Government of India'
    
    def _extract_date(self, text):
        if not text:
            return 'Check notification'
        patterns = [
            r'Last Date[:\s]+(\d{1,2}[-/\.]\d{1,2}[-/\.]\d{2,4})',
            r'(\d{1,2}[-/\.]\d{1,2}[-/\.]\d{4})',
            r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{4})'
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1) if len(match.groups()) == 1 else match.group(0)
        return 'Check notification'
    
    def _extract_qualification(self, text):
        if not text:
            return 'As per notification'
        text_upper = text.upper()
        quals = [('10th Pass', ['10TH', 'MATRIC']), ('12th Pass', ['12TH', 'INTERMEDIATE']), 
                 ('Graduate', ['GRADUATE', 'DEGREE', 'B.A', 'B.SC', 'B.COM']), 
                 ('Post Graduate', ['PG', 'POST GRADUATE', 'M.A', 'M.SC', 'MBA'])]
        for qual_name, keywords in quals:
            for kw in keywords:
                if kw in text_upper:
                    return qual_name
        return 'As per notification'
    
    async def fetch_all_jobs(self):
        logger.info("=" * 60)
        logger.info("STARTING MASSIVE JOB FETCH")
        logger.info("=" * 60)
        
        await self.init_session()
        all_jobs = []
        
        # Try RSS first
        for name, url in self.rss_sources.items():
            jobs = await self.fetch_rss(url, name)
            all_jobs.extend(jobs)
            await asyncio.sleep(0.5)  # Fast but polite
        
        # Fallback to HTML if few jobs found
        if len(all_jobs) < 5:
            logger.info("Few RSS jobs, trying HTML sources...")
            for name, url in self.html_sources.items():
                jobs = await self.fetch_html(url, name)
                all_jobs.extend(jobs)
                await asyncio.sleep(1)
        
        await self.close_session()
        
        logger.info(f"TOTAL JOBS FOUND: {len(all_jobs)}")
        logger.info("=" * 60)
        
        return all_jobs

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
• Application Fee: As per category
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
🤖 Bot: @{(os.getenv("BOT_USERNAME") or "YourBot").replace('@', '')}
📢 Jobs every 30 minutes!
⏰ Posted: {datetime.now().strftime('%d-%m-%Y %H:%M')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

def get_buttons(job):
    keyboard = [
        [InlineKeyboardButton("🚀 APPLY NOW", url=job.get('apply_link', 'https://employmentnews.gov.in'))],
        [
            InlineKeyboardButton("📋 FULL DETAILS", callback_data=f"details_{job['id']}"),
            InlineKeyboardButton("📅 DATES", callback_data=f"dates_{job['id']}")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

# ==================== BOT HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Welcome message with add buttons"""
    user = update.effective_user
    
    welcome_text = f"""
👋 *Welcome {user.first_name}!*

🤖 *I'm Government Jobs Bot*

🇮🇳 I provide instant alerts for:
• SSC, UPSC, Railway, Banking
• Defence, Police, Teaching
• State PSCs (All States)
• PSU, Central Govt Jobs

⚡ *Features:*
✅ Auto-post every 30 minutes
✅ 50+ official sources
✅ Complete A-G format
✅ Direct apply links

📌 *Add me to your channel/group:*
• Make me admin
• I'll auto-post jobs
• No verification needed!
"""
    
    keyboard = [
        [InlineKeyboardButton("➕ Add to Channel/Group", url=f"https://t.me/{context.bot.username}?startgroup=true")],
        [InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/justrobodude")],
        [InlineKeyboardButton("📢 View Demo Channel", url="https://t.me/Roboallbotchannel")]
    ]
    
    await update.message.reply_text(
        welcome_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """
🤖 *How to Use Me:*

*For Channels/Groups:*
1. Add me as admin
2. Give "Post Messages" permission
3. I'll auto-post jobs every 30 min!

*Commands:*
/help - This message
/update - Admin: Fetch jobs now
/stats - View bot statistics

*Need Help?* Contact @justrobodude
"""
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def update_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: Manual fetch"""
    user = update.effective_user
    
    # Check if admin or channel admin
    is_admin = user.id == int(os.getenv("ADMIN_ID", "0"))
    
    if not is_admin:
        # Check if channel admin
        chat = update.effective_chat
        if chat.type in ['group', 'supergroup', 'channel']:
            try:
                member = await context.bot.get_chat_member(chat.id, user.id)
                if member.status not in ['administrator', 'creator']:
                    await update.message.reply_text("❌ Admin only!")
                    return
            except:
                await update.message.reply_text("❌ Error checking admin status!")
                return
        else:
            await update.message.reply_text("❌ This command only works in channels/groups!")
            return
    
    await update.message.reply_text("🔄 Fetching jobs from 50+ sources...")
    
    scraper = JobScraper()
    jobs = await scraper.fetch_all_jobs()
    
    new_count = 0
    for job in jobs:
        if db.add_job(job):
            new_count += 1
    
    # Post to current chat if it's a channel/group
    chat = update.effective_chat
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
        f"✅ Done!\n"
        f"📊 Found: {len(jobs)} jobs\n"
        f"🆕 New: {new_count} jobs\n"
        f"📢 Posted: {min(new_count, 3)}"
    )

async def chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """When bot is added to channel/group"""
    result = update.my_chat_member
    
    if result.new_chat_member.status == 'administrator':
        chat = result.chat
        logger.info(f"Bot added to {chat.title} ({chat.id})")
        
        # Save to database
        db.add_channel(chat.id, chat.title, chat.type, result.from_user.id)
        
        # Send welcome message
        try:
            await context.bot.send_message(
                chat_id=chat.id,
                text=f"""
🎉 *Bot Activated!*

✅ I'll post government job alerts every 30 minutes!

📌 *Commands:*
/update - Fetch jobs now
/help - Help message

🤖 Managed by @justrobodude
""",
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Welcome msg failed: {e}")

# ==================== AUTO POST TO ALL CHANNELS ====================
async def auto_fetch_and_broadcast(context: ContextTypes.DEFAULT_TYPE):
    """Fetch and post to ALL channels/groups every 30 min"""
    logger.info("=" * 60)
    logger.info("AUTO BROADCAST STARTED")
    logger.info("=" * 60)
    
    scraper = JobScraper()
    jobs = await scraper.fetch_all_jobs()
    
    new_count = 0
    for job in jobs:
        if db.add_job(job):
            new_count += 1
    
    logger.info(f"New jobs: {new_count}")
    
    if new_count > 0:
        # Get all channels
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
        
        # Mark as posted
        for job in unposted:
            db.mark_posted(job['id'])
    
    logger.info("AUTO BROADCAST COMPLETED")

# ==================== MAIN ====================
def main():
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("update", update_command))
    
    # When bot is added to channel/group
    application.add_handler(ChatMemberHandler(chat_member_update, ChatMemberHandler.MY_CHAT_MEMBER))
    
    # Scheduled job every 30 minutes
    job_queue = application.job_queue
    job_queue.run_repeating(auto_fetch_and_broadcast, interval=timedelta(minutes=30), first=10)
    
    logger.info("Bot started! Waiting for channels/groups...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
