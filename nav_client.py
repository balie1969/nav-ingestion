import requests
import logging
import time

class NavClient:
    """
    Client for the NAV 'pam-stilling-feed' API.
    Documentation: https://pam-stilling-feed.ekstern.dev.nav.no/redoc
    """
    DEFAULT_API_URL = "https://pam-stilling-feed.nav.no"

    def __init__(self, api_token, api_url=None):
        self.api_url = api_url or self.DEFAULT_API_URL
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_token}",
            "Accept": "application/json",
            "User-Agent": "jobsai-backend/1.0" 
        })
        self.logger = logging.getLogger(__name__)

    def _make_request(self, method, endpoint, params=None, retries=3):
        url = f"{self.api_url}{endpoint}"
        for attempt in range(retries):
            try:
                response = self.session.request(method, url, params=params)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt+1}/{retries}): {e}")
                if attempt == retries - 1:
                    self.logger.error(f"Failed to fetch {url} after {retries} attempts")
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff

    def get_feed_page(self, page_id=None, last=False):
        """
        Fetches a page of the feed.
        If page_id is provided, fetches that specific page.
        If last=True, fetches the last (newest) page.
        Default is the first page.
        """
        endpoint = "/api/v1/feed"
        if page_id:
            endpoint = f"/api/v1/feed/{page_id}"
            params = None
        else:
            params = {"last": "true"} if last else None
        
        return self._make_request("GET", endpoint, params=params)

    def get_feed_entry(self, entry_id):
        """
        Fetches details for a specific feed entry (job).
        """
        endpoint = f"/api/v1/feedentry/{entry_id}"
        data = self._make_request("GET", endpoint)
        # Debug: Log keys of the response to understand missing ad_content
        # self.logger.info(f"Debug Raw Entry {entry_id} Keys: {data.keys()}")
        return data

    def fetch_feed_pages(self, start_page_id=None, last=False):
        """
        Generator that yields (page_data, distinct_job_items).
        This allows the caller to handle state updates (next_url) per page.
        """
        current_page_id = start_page_id

        # First fetch
        if not current_page_id:
            page_data = self.get_feed_page(last=last)
        else:
            page_data = self.get_feed_page(page_id=current_page_id)

        while True:
            # Yield current page data to caller processing
            yield page_data

            # Pagination
            next_id = page_data.get("next_id")
            if not next_id:
                break
            
            # self.logger.info(f"Fetching next page: {next_id}") # Logged in loop usually
            current_page_id = next_id
            page_data = self.get_feed_page(page_id=current_page_id)
