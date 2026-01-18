from src.monitor.notifier import DiscordNotifier

if __name__ == "__main__":
    notifier = DiscordNotifier()
    print("Sending test message to Discord...")
    notifier._send({
        "embeds": [{
            "title": "🚀 Bybit Monitor 測試",
            "description": "這是一條測試訊息，代表您的 Webhook 設定成功！",
            "color": 3447003, # Blue
            "fields": [
                {"name": "狀態", "value": "✅ 連線正常", "inline": True},
                {"name": "監控項目", "value": "掛單 / 成交 / 持倉", "inline": True}
            ]
        }]
    })
    print("Test message sent!")
