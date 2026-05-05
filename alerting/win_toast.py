"""Windows 桌面通知（PowerShell + Windows.UI.Notifications，零 pip 依赖）."""

import subprocess
import logging
import sys

logger = logging.getLogger(__name__)


class DesktopNotifier:
    """Send Windows 10/11 toast notifications."""

    APP_NAME = "Arbitrage Monitor"

    def __init__(self, config: dict):
        ac = config.get("alerts", {})
        dc = ac.get("desktop", {})
        self.enabled: bool = dc.get("enabled", False) and sys.platform == "win32"

    def send(self, title: str, body: str) -> bool:
        """Send a Windows toast notification. Returns True if the PowerShell call succeeded."""
        if not self.enabled:
            return False

        # Escape special characters for PowerShell
        ps_title = title.replace("'", "''")
        ps_body = body.replace("'", "''")

        script = (
            f'[Windows.UI.Notifications.ToastNotificationManager,'
            f'Windows.UI.Notifications,ContentType=WindowsRuntime] > $null;'
            f'$t=[Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent('
            f'[Windows.UI.Notifications.ToastTemplateType]::ToastText02);'
            f'$x=New-Object Windows.Data.Xml.Dom.XmlDocument;'
            f'$x.LoadXml($t.GetXml());'
            f'$x.GetElementsByTagName("text")[0].AppendChild('
            f'$x.CreateTextNode("{ps_title}")) > $null;'
            f'$x.GetElementsByTagName("text")[1].AppendChild('
            f'$x.CreateTextNode("{ps_body}")) > $null;'
            f'$n=[Windows.UI.Notifications.ToastNotification]::new($x);'
            f'[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier('
            f'"{self.APP_NAME}").Show($n)'
        )

        try:
            result = subprocess.run(
                ["powershell", "-Command", script],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=10,
            )
            if result.returncode == 0:
                logger.debug("Desktop notification sent")
                return True
            else:
                logger.warning("Desktop notification failed: %s", result.stderr.strip())
                return False
        except Exception as e:
            logger.error("Desktop notification error: %s", e)
            return False
