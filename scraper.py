import aiohttp
import feedparser
import logging
from datetime import datetime
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JobScraper:
    def __init__(self):
        self.session = None
        self.sources = {
            'employment_news': 'https://employmentnews.gov.in/rss-feed',
            'ssc': 'https://ssc.nic.in/rss-feed',
            'upsc': 'https://upsc.gov.in/rss-feed',
            'tnpsc': 'https://tnpsc.gov.in/rss-feed',
            'uppsc': 'https://uppsc.up.nic.in/rss'
        }
    
    async def init_session(self):
        """Create aiohttp session with headers"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session = aiohttp.ClientSession(headers=headers)
        logger.info("Session initialized")
    
    async def close_session(self):
        """Close session"""
        if self.session:
            await self.session.close()
            logger.info("Session closed")
    
    async def fetch_rss(self, url, source_name):
        """Fetch and parse RSS feed"""
        try:
            logger.info(f"Fetching {source_name}...")
            async with self.session.get(url, timeout=30) as response:
                if response.status != 200:
                    logger.error(f"{source_name}: HTTP {response.status}")
                    return []
                
                content = await response.text()
                feed = feedparser.parse(content)
                
                jobs = []
                for entry in feed.entries[:5]:  # Get latest 5 only
                    job = self._parse_entry(entry, source_name)
                    if job:
                        jobs.append(job)
                
                logger.info(f"{source_name}: Found {len(jobs)} jobs")
                return jobs
                
        except Exception as e:
            logger.error(f"{source_name} error: {str(e)}")
            return []
    
    def _parse_entry(self, entry, source):
        """Parse RSS entry to job dict"""
        title = entry.get('title', '').strip()
        summary = entry.get('summary', '').strip()
        link = entry.get('link', '').strip()
        
        if not title:
            return None
        
        # Extract organization
        org = self._extract_org(title)
        
        # Extract last date
        last_date = self._extract_date(summary) or self._extract_date(title)
        
        # Extract qualification
        qual = self._extract_qualification(summary)
        
        return {
            'source': source,
            'title': title,
            'organization': org,
            'qualification': qual,
            'last_date': last_date,
            'apply_link': link,
            'notification_link': link,
            'post_date': entry.get('published', str(datetime.now())),
            'location': 'All India',
            'raw_summary': summary[:500]  # Store for debugging
        }
    
    def _extract_org(self, title):
        """Extract organization from title"""
        orgs = {
            'SSC': ['SSC', 'Staff Selection'],
            'UPSC': ['UPSC', 'Union Public Service'],
            'RRB': ['RRB', 'Railway', 'RRC'],
            'IBPS': ['IBPS', 'Banking'],
            'NVS': ['NVS', 'Navodaya'],
            'ESIC': ['ESIC'],
            'BECIL': ['BECIL'],
            'SAIL': ['SAIL'],
            'UPPSC': ['UPPSC'],
            'TNPSC': ['TNPSC'],
            'BPSC': ['BPSC'],
            'MPPSC': ['MPPSC']
        }
        
        title_upper = title.upper()
        for org, keywords in orgs.items():
            for kw in keywords:
                if kw.upper() in title_upper:
                    return org
        return 'Government of India'
    
    def _extract_date(self, text):
        """Extract last date from text"""
        if not text:
            return 'Check notification'
        
        patterns = [
            r'Last Date[:\s]+(\d{1,2}[-/\.]\d{1,2}[-/\.]\d{2,4})',
            r'Apply before[:\s]+(\d{1,2}[-/\.]\d{1,2}[-/\.]\d{2,4})',
            r'Closing Date[:\s]+(\d{1,2}[-/\.]\d{1,2}[-/\.]\d{2,4})',
            r'(\d{1,2}[-/\.]\d{1,2}[-/\.]\d{4})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return 'Check notification'
    
    def _extract_qualification(self, text):
        """Extract qualification from text"""
        if not text:
            return 'As per notification'
        
        text_upper = text.upper()
        
        quals = [
            ('10th Pass', ['10TH', 'MATRIC', 'SECONDARY', 'HIGH SCHOOL']),
            ('12th Pass', ['12TH', 'INTERMEDIATE', 'HIGHER SECONDARY', '10+2']),
            ('Graduate', ['GRADUATE', 'DEGREE', 'B.A', 'B.SC', 'B.COM', 'B.TECH']),
            ('Post Graduate', ['POST GRADUATE', 'PG', 'M.A', 'M.SC', 'M.COM', 'MBA']),
            ('Diploma', ['DIPLOMA', 'POLYTECHNIC']),
            ('ITI', ['ITI'])
        ]
        
        for qual_name, keywords in quals:
            for kw in keywords:
                if kw in text_upper:
                    return qual_name
        return 'As per notification'
    
    async def fetch_all_jobs(self):
        """Fetch jobs from all sources"""
        logger.info("=" * 50)
        logger.info("STARTING JOB FETCH")
        logger.info("=" * 50)
        
        await self.init_session()
        
        all_jobs = []
        
        # Fetch from all RSS sources
        for name, url in self.sources.items():
            jobs = await self.fetch_rss(url, name)
            all_jobs.extend(jobs)
            await asyncio.sleep(1)  # Be polite
        
        await self.close_session()
        
        logger.info("=" * 50)
        logger.info(f"TOTAL JOBS FOUND: {len(all_jobs)}")
        logger.info("=" * 50)
        
        return all_jobs


# Standalone test
if __name__ == "__main__":
    import asyncio
    
    async def test():
        scraper = JobScraper()
        jobs = await scraper.fetch_all_jobs()
        
        print(f"\n{'='*50}")
        print(f"FOUND {len(jobs)} JOBS")
        print(f"{'='*50}\n")
        
        for i, job in enumerate(jobs[:3], 1):
            print(f"{i}. {job['title']}")
            print(f"   Org: {job['organization']}")
            print(f"   Date: {job['last_date']}")
            print(f"   Qual: {job['qualification']}")
            print()
    
    asyncio.run(test())
