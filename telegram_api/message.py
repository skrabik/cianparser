"""
    Класс реализующий отправку сообщения
    И др.
"""

import requests


class Message:
    def __init__(self, update_id=None, message_id=None, text=None, chat_id=None):
        self.update_id = update_id
        self.message_id = message_id
        self.text = text
        self.chat_id = chat_id

class StaticMethods:
    """
    Класс со статическими методами для обработки сообщений
    """
    @staticmethod
    def parseMessage(update):
        return Message(update_id=update['update_id'], message_id=update['message']['message_id'], text=update['message']['text'], chat_id=update['message']['chat']['id'])

    @staticmethod
    def sendText(BOT_TOKEN, chat_id, text=''):
        print(f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage?chat_id={chat_id}&text={text}')
        return requests.get(f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage?chat_id={chat_id}&text={text}')

    @staticmethod
    def sendPhoto(BOT_TOKEN, chat_id, photo_path):
        files = {'photo': open(photo_path, 'rb')}
        data = {'chat_id': chat_id}
        return requests.post(f'https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto', files=files, data=data)

    @staticmethod
    def getStartOffset(URL):
        request = requests.get(f'{URL}getUpdates')

        while not request.json()['result']:
            request = requests.get(f'{URL}getUpdates')

        return request.json()['result'][0]['update_id']