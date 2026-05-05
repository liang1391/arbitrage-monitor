"""Server酱 Turbo → 微信推送."""

import logging
import os
import requests

logger = logging.getLogger(__name__)


class ServerChanAlerter:
    """Send alerts to WeChat via Server酱 Turbo (supports multiple sendkeys)."""

    API_URL = "https://sctapi.ftqq.com/{sendkey}.send"

    def __init__(self, config: dict):
        ac = config.get("alerts", {})
        wc = ac.get("wechat", {})
        self.enabled: bool = wc.get("enabled", False)
        self.timeout: int = 30

        # Env var override (for CI/CD): WECHAT_SENDKEYS=key1,key2
        env_keys = os.environ.get("WECHAT_SENDKEYS", "")
        if env_keys:
            keys = [k.strip() for k in env_keys.split(",") if k.strip()]
        else:
            key = wc.get("sendkey", "")
            keys = wc.get("sendkeys", [])
            if key:
                keys.append(key)
        self.sendkeys: list[str] = list(dict.fromkeys(keys))  # dedupe, preserve order

    def send(self, title: str, body: str) -> bool:
        """Send WeChat push to ALL configured sendkeys. Returns True if any succeeded."""
        if not self.enabled:
            logger.debug("WeChat alerting disabled, skipping")
            return False
        if not self.sendkeys:
            logger.warning("Server酱 sendkey not configured")
            return False

        any_ok = False
        for sk in self.sendkeys:
            url = self.API_URL.format(sendkey=sk)
            try:
                resp = requests.post(url, data={
                    "title": title,
                    "desp": body,
                }, timeout=self.timeout)
                result = resp.json()
                if result.get("code") == 0:
                    logger.info("Server酱 sent: %s", title)
                    any_ok = True
                else:
                    logger.warning("Server酱 failed for %s: %s", sk[:12], result)
            except requests.RequestException as e:
                logger.error("Server酱 request failed for %s: %s", sk[:12], e)

        return any_ok
