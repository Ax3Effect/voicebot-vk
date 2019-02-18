from celery import Celery
import time
import vk_api
from vk_api.keyboard import VkKeyboard, VkKeyboardColor
import json
import requests
import tempfile

from flask import request
from pymongo import MongoClient
import pymongo
from flask import Flask
import time
from datetime import datetime, date, timedelta
from datetime import time as timed
import collections
import random
import os
import glob
from pydub import AudioSegment
client = MongoClient(connect=False)
db = client.vkstats2

celery = Celery('tasks', broker='pyamqp://guest@localhost//')



# settings
vk_api_key = os.environ.get('vk_api_key')
webhook_return_code = "2adc56cf"
webhook_group_id = "169308824"
webhook_group_type = "club"
webhook_group_name = "Статистика Конфы"
sounds_folder = "sounds"



class EventAdder:
    def __init__(self):
        self.users = db.users
        self.users_settings = db.users_settings
        self.messages = db.messages
        self.debug = False
        self.audio_changer = True

    def add_user(self, id):
        # if users name is not found in db, get one from vk
        if id < 0:
            return None
        user = self.users.find_one({"id": id})
        if not user:
            
            user = vk.users.get(user_ids=id)
            self.users.insert_one(user[0])
            return user[0]

        else:
            return user

    def process_msg(self, message):
        user = self.get_user(message["from_id"])
        if self.debug:
            print(message)
        else:
            if user:
                print("[{}] {} {}: {}".format(message["peer_id"] - 2000000000, user["first_name"], user["last_name"], message.get("text", None)))
            else:
                print("{}: {}".format(message["from_id"], message.get("text", None)))

        if message["peer_id"] == message["from_id"]:
            self.message_from_cp(message)
        else:
            self.message_from_group(message)
            self.add_message(message)

    def add_message(self, message):
        # Get user from userid and save message to db
        if str(message["from_id"]).startswith("-"):
            return
        user = self.get_user(message["from_id"])
        message_struct = {
            "id":message["conversation_message_id"],
            "date":message["date"],
            "from_id":message["from_id"],
            "group_id":message["peer_id"] - 2000000000,
            "first_name":user["first_name"],
            "last_name":user["last_name"],
            "text":message.get("text", None),
            "attachments":message.get("attachments", None),
            "fwd_messages":message.get("fwd_messages", None)
        }
        msg = self.messages.insert_one(message_struct)

    def message_from_group(self, message):
        user = self.users_settings.find_one({"user_id": message["from_id"]})
        if user:
            if user["on"] == False:
                return

        self.process_switches(message)
        self.process_stats(message)
        if self.messages.find({"group_id":message["peer_id"]-2000000000}).count() < 1:
            pass
            #self.send_intro(message)

        if self.audio_changer:
            if message.get("attachments", None):
                self.process_audio(message)
                if message["attachments"][0]["type"] == "sticker":
                    if message["attachments"][0]["sticker"]["sticker_id"] == 4452:
                        pudge_list = ['15']
                        pudge = random.choice(pudge_list)
                        audio_msg = upload.audio_message("Pud_ability_devour_{}.mp3".format(pudge), group_id="169308824")
                        attachm = "doc{}_{}".format(audio_msg[0]["owner_id"], audio_msg[0]["id"])
                        vk.messages.send(peer_id=message["peer_id"], attachment=attachm)
            elif message.get("fwd_messages", None):
                if message["fwd_messages"][0].get("attachments", None):
                    if message["text"] == "":
                        self.process_audio(message, message["peer_id"])
        return True
    
    def message_from_cp(self, message):
        # checking if it's first time messaging
        user = self.users_settings.find_one({"user_id": message["from_id"]})

        if not user:
            initial_payload = {
                "user_id": message["from_id"],
                "state": 1,
                "current_bassboost": 0,
                "current_pitch": -0.5,
                "on" : True
            } 
            msg = self.users_settings.insert_one(initial_payload)

            # TODO: send first time msg

        processed = self.process_keyboard(message)
        user = self.users_settings.find_one({"user_id": message["from_id"]})

        msg_to_user = '''
        НАСТРОЙКИ ДЛЯ КОНФЫ
        on = {}
        bassboost = {}
        pitch = {}

        '''.format(user["on"], user["current_bassboost"], user["current_pitch"])

        if not processed:
            keyboard = self.keyboard_init(message)
            vk.messages.send(peer_id=message["peer_id"], message=msg_to_user, keyboard=keyboard)
        else:
            vk.messages.send(peer_id=message["peer_id"], message=msg_to_user)
            pass
            #vk.messages.send(peer_id=message["peer_id"], message=msg_to_user)

    def keyboard_init(self, message):
        keyboard = VkKeyboard(one_time=False)
        keyboard.add_button("on", color=VkKeyboardColor.PRIMARY)
        keyboard.add_button("off", color=VkKeyboardColor.PRIMARY)
        keyboard.add_line()
        keyboard.add_button('bassboost+', color=VkKeyboardColor.POSITIVE)
        keyboard.add_button('bassboost-', color=VkKeyboardColor.NEGATIVE)
        keyboard.add_line() 
        keyboard.add_button('pitch+', color=VkKeyboardColor.POSITIVE)
        keyboard.add_button('pitch-', color=VkKeyboardColor.NEGATIVE)
        
        return keyboard.get_keyboard()
        
    def process_keyboard(self, message):
        text = message["text"]

        if text == "bassboost+":
            self.change(message, "bassboost", "+")
            return True
        elif text == "bassboost-":
            self.change(message, "bassboost", "-")
            return True
        elif text == "pitch+":
            self.change(message, "pitch", "+")
            return True
        elif text == "pitch-":
            self.change(message, "pitch", "-")
            return True
        elif text == "on":
            self.change(message, "on", "true")
            return True
        elif text == "off":
            self.change(message, "on", "false")
            return True
        elif text.startswith("msgconf"):
            empty, empty2, text_to_conf = text.partition(' ')
            vk.messages.send(message=text_to_conf, peer_id=2000000001)
            return True
        elif text.startswith("audioconf"):
            empty, empty2, text_to_conf = text.partition(' ')
            group_id = text_to_conf.split()[0]
            if group_id == "list":
                mp3list = ""
                for file in glob.glob("*.mp3"):
                    mp3list += file + "\n"
                vk.messages.send(peer_id=message["peer_id"], message=mp3list)
                return True
            mp3file = text_to_conf.split()[1]
            audio_msg = upload.audio_message("{}.mp3".format(mp3file), group_id="169308824")
            attachm = "doc{}_{}".format(audio_msg[0]["owner_id"], audio_msg[0]["id"])
            vk.messages.send(peer_id=2000000000+int(group_id), attachment=attachm)
        elif text.startswith("rr"):
            empty, empty2, text_to_conf = text.partition(' ')
            group_id = text_to_conf.split()[0]
            text_to_send = text_to_conf.split(' ', 1)[1]
            vk.messages.send(peer_id=2000000000+int(group_id), message=text_to_send)


        elif text.startswith("listconf"):
            all_confs = self.messages.find({"from_id":message["from_id"]}).distinct("group_id")
            vk.messages.send(peer_id=message["peer_id"], message=str(all_confs))
            return True
        else:
            return False

    def change(self, message, key, value):
        user = self.users_settings.find_one({"user_id": message["from_id"]})

        defaults = {}
        # minimum, maximum, step
        min_max = {"bassboost": (-30, 30, 10),
                    "pitch": (-2, 2, 0.5)}

        if key == "bassboost":
            current_bassboost = user["current_bassboost"]
            mins = min_max["bassboost"]
            if value == "+":
                if current_bassboost + mins[2] > mins[1]:
                    self.send_q(message, "already at max")
                    return

                self.users_settings.update_one({
                    '_id': user["_id"],
                }, {
                    '$set': {
                        "current_bassboost": current_bassboost + mins[2]
                    }
                })
            
            if value == "-":
                if current_bassboost - mins[2] < mins[0]:
                    self.send_q(message, "already at minimum")
                    return

                self.users_settings.update_one({
                    '_id': user["_id"],
                }, {
                    '$set': {
                        "current_bassboost": current_bassboost - mins[2]
                    }
                })
        if key == "pitch":
            current_pitch = user["current_pitch"]
            mins = min_max["pitch"]
            if value == "+":
                if current_pitch + mins[2] > mins[1]:
                    self.send_q(message, "already at max")
                    return
                self.users_settings.update_one({
                    '_id': user["_id"],
                }, {
                    '$set': {
                        "current_pitch": current_pitch + mins[2]
                    }
                })

            if value == "-":
                if current_pitch - mins[2] < mins[0]:
                    self.send_q(message, "already at minimum")
                    return

                self.users_settings.update_one({
                    '_id': user["_id"],
                }, {
                    '$set': {
                        "current_pitch": current_pitch - mins[2]
                    }
                })
        if key == "on":
            if value == "true":
                self.users_settings.update_one({
                    '_id': user["_id"],
                }, {
                    '$set': {
                        "on": True
                    }
                })
            if value == "false":
                self.users_settings.update_one({
                    '_id': user["_id"],
                }, {
                    '$set': {
                        "on": False
                    }
                })

            
    def send_intro(self, message):
        intro_msg = '''Приветствую вас, свежее мясцо! 
        
        Pudge может замедлять, ускорять и бассбустить аудиосообщения. Чтобы поменять значения, напишите ему в лс. Также Pudge отзывается на слова, словарь потихоньку будет пополняться со временем. 
        Ещё Pudge может показывать статистику конфы, для этого напишите "стата". Со временем будут добавляться новые фичи. Если будут какие-то вопросы, пиши в комментариях.
        '''
        intro_photo = upload.photo_messages("intro_msg.png")
        attachm = "photo{}_{}".format(intro_photo[0]["owner_id"], intro_photo[0]["id"])
        
        #print("msg send")
        vk.messages.send(peer_id=message["peer_id"], message=intro_msg, attachment=attachm)

    def send_audio(self, message, mp3):
        audio_msg = upload.audio_message("{}/{}.mp3".format(sounds_folder, mp3), group_id="169308824")
        attachm = "doc{}_{}".format(audio_msg[0]["owner_id"], audio_msg[0]["id"])
        vk.messages.send(peer_id=message["peer_id"], attachment=attachm)

    def send_q(self, message, text):
        vk.messages.send(peer_id=message["peer_id"], message=text)

    def process_audio(self, message, peer_id=None):
        orig_msg = message
        if (message["peer_id"] - 2000000000) == 254:
            return
        if message.get("fwd_messages",None):
            message = message["fwd_messages"][0]
        if message.get("attachments", None):
            if message["attachments"][0]["type"] == "doc":
                if message["attachments"][0]["doc"].get("preview", None).get("audio_msg", None):
                    if message["attachments"][0]["doc"]["preview"]["audio_msg"]["duration"] > 200:
                        return
                    audio_message_url = message["attachments"][0]["doc"]["url"]
                    r = requests.get(audio_message_url)
                    with open('audio_file.ogg', 'wb') as f:  
                        f.write(r.content)

                    sound = AudioSegment.from_file("audio_file.ogg", format="ogg")

                    user_settings = self.users_settings.find_one({"user_id": orig_msg["from_id"]})
                    if user_settings:
                        octaves = user_settings["current_pitch"]
                        sound = sound + user_settings["current_bassboost"]
                    else:
                        octaves = -0.5

                    new_sample_rate = int(sound.frame_rate * (2.0 ** octaves))
                    lowpitch_sound = sound._spawn(sound.raw_data, overrides={'frame_rate': new_sample_rate})
                    lowpitch_sound.export("edited.ogg", format="ogg")
                    
                    audio_msg = upload.audio_message("edited.ogg", group_id="169308824")

                    attachm = "doc{}_{}".format(audio_msg[0]["owner_id"], audio_msg[0]["id"])
                    if peer_id == None:
                        peer_id = message["peer_id"]
                    try:
                        vk.messages.send(peer_id=peer_id, attachment=attachm)
                    except Exception as e:
                        print("msg send fail: {}".format(e))

    def get_user(self, id):
        return self.add_user(id)

    def process_switches(self, message):
        lower = message["text"].lower()
        if message["text"] == "debug":
            if int(message["from_id"]) == 10399749:
                current_time = time.time()
                time_for = current_time + 24*60*60
                debug_info = '''time_for = {}
                current_time = {}
                message time = {}'''.format(time_for, current_time, message["date"])
                vk.messages.send(peer_id=message["peer_id"], message=debug_info)
        if message["text"] == "[club169308824|@bot_groupstats] debug on":
            if int(message["from_id"]) == 10399749:
                self.debug = True
                vk.messages.send(peer_id=message["peer_id"], message="debug on")
        if message["text"] == "[club169308824|@bot_groupstats] debug off":
            if int(message["from_id"]) == 10399749:
                self.debug = False
                vk.messages.send(peer_id=message["peer_id"], message="debug off")
        if message["text"] == "[club169308824|@bot_groupstats] audio on":
            if int(message["from_id"]) == 10399749:
                self.audio_changer = True
                vk.messages.send(peer_id=message["peer_id"], message="audio on")
        if message["text"] == "[club169308824|@bot_groupstats] audio off":
            if int(message["from_id"]) == 10399749:
                self.audio_changer = False
                vk.messages.send(peer_id=message["peer_id"], message="audio off")
        if message["text"].startswith("[club169308824|@bot_groupstats] ban"):
            if int(message["from_id"]) == 10399749:
                ban_id = message["text"].split()[2]
                self.users_settings.update_one({
                    'user_id': ban_id,
                }, {
                    '$set': {
                        "on": False
                    }
                })
                vk.messages.send(peer_id=message["peer_id"], message=" zabanen 😂🤙🏻")
        if message["text"].startswith("[club169308824|@bot_groupstats] unban"):
            if int(message["from_id"]) == 10399749:
                ban_id = message["text"].split()[2]
                self.users_settings.update_one({
                    'user_id': ban_id,
                }, {
                    '$set': {
                        "on": True
                    }
                })
                vk.messages.send(peer_id=message["peer_id"], message=" razbanen 😂🤙🏻")

        if "нормально" in lower:
            r = random.randint(0, 1)
            if r == 0:
                audio_msg = self.send_audio(message, "Pud_thanks_02_ru")
            else:
                audio_msg = self.send_audio(message, "Pud_thanks_02_ru_no")
            return
        if "чин чопа" in lower:
            self.send_audio(message, "Pud_respawn_05")
            return
        if "ножки" in lower:
            self.send_audio(message, "Pud_respawn_06_ru")
            return
        if "фортнайт" in lower:
            self.send_audio(message, "fortnite_earrape")
            return
        if "хочу жрать" in lower:
            self.send_audio(message, "Pud_rare_05_ru")
            return
        if "бухло" in lower:
            self.send_audio(message, "Pud_rival_10_ru")
            return
        if "шаверма" in lower or "шаурма" in lower:
            self.send_audio(message, "Pud_respawn_04_ru")
            return
        if "мясо" in lower:
            r = random.randint(0, 1)
            if r == 0:
                audio_msg = self.send_audio(message, "Pud_ability_devour_03_ru")
            else:
                audio_msg = self.send_audio(message, "Pud_rival_01_ru")
            return
        if "битбокс" in lower:
            self.send_audio(message, "d09edca8fe2a06")
            return
        if "гучи мейн" in lower:
            self.send_audio(message, "gimmeloot")
            return
            
        

    def process_stats(self, message):
        if message["text"] == "стата":
            stats = self.generate_stats(message)
            vk.messages.send(peer_id=message["peer_id"], message=stats)

    def generate_stats(self, message):
        group_id = message["peer_id"] - 2000000000
        current_time = int(time.time())
        time_for = current_time - 24*60*60
            # get start of today
        dt = datetime.combine(date.today(), timed(0, 0, 0))
        # start of yesterday = one day before start of today
        sday_timestamp = int((dt - timedelta(days=1)).timestamp())
        # end of yesterday = one second before start of today
        eday_timestamp = int((dt - timedelta(seconds=1)).timestamp())

        yesterday_messages = db.messages.find({'group_id':group_id, 'date':{'$gt':sday_timestamp, '$lt':eday_timestamp}}).count()

        all_messages = db.messages.find({'group_id':group_id, 'date':{'$gt':time_for}}).sort([("from_id", pymongo.DESCENDING)])
        result = "💬За последние 24 часа: {}\n За вчерашний день: {}\Написали мне тут:\n".format(all_messages.count(), yesterday_messages)
        segregate_users = all_messages.limit(10).distinct("from_id")
        top_users = []
        for user in segregate_users:
            msgs_from_user = db.messages.find({'group_id':group_id, 'date':{'$gt':time_for}, 'from_id':user})
            text_list = []
            msgs_from_user_text = db.messages.find({'group_id':group_id, 'date':{'$gt':time_for}, 'from_id':user})
            for msgs in msgs_from_user_text:
                if msgs["text"]:
                    splitted_msg = msgs["text"].split()
                    

                    y = [s for s in splitted_msg if len(s) > 3]
                    text_list.extend(y)

            counter = collections.Counter(text_list)
            top_users.append((user, msgs_from_user.count(), msgs_from_user, counter.most_common()))

        sorted_top_users = sorted(top_users, key=lambda tup: tup[1], reverse=True)
        for i in sorted_top_users[:10]:
            #print(i[3])
            try:
                result += "{} {}.: {} сбщ. ({} - {} раз, {} - {} раз, {} - {} раз)\n".format(i[2][0]["first_name"], i[2][0]["last_name"][0], i[1], i[3][0][0][:100], i[3][0][1], i[3][1][0][:100], i[3][1][1], i[3][2][0][:100], i[3][2][1])
            except IndexError:
                pass

        last_hour = current_time - 60*60
        top1hour_messages = db.messages.find({'group_id':group_id, 'date':{'$gt':last_hour}}).sort([("from_id", pymongo.DESCENDING)])
        if top1hour_messages:
            top_users_segregation = top1hour_messages.limit(5).distinct("from_id")
            top_users_1hr = []
            for user in segregate_users:
                msgs_from_user = db.messages.find({'group_id':group_id, 'date':{'$gt':last_hour}, 'from_id':user})
                top_users_1hr.append((user, msgs_from_user.count(), msgs_from_user))

            sorted_1hr = sorted(top_users_1hr, key=lambda tup: tup[1], reverse=True)
            try:
                result += "За последний час больше всего написал: {} {} ({} сбщ)".format(sorted_1hr[0][2][0]["first_name"], sorted_1hr[0][2][0]["last_name"], sorted_1hr[0][1])
            except Exception:
                pass

        return result

event = EventAdder()

vk_session = vk_api.VkApi(token=vk_api_key)
vk = vk_session.get_api()
upload = vk_api.VkUpload(vk_session)


@celery.task
def add(content):
    if content["type"] == "message_new":
        message = content["object"]
        event.add_user(message["from_id"])
        event.process_msg(message)
