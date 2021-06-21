import time
from threading import Thread
import pygame
import io
import logging


class Player(Thread):
    message_buffer = None
    prefix_buffer = None

    def __init__(self, message_buffer, prefix_buffer = None):
        Thread.__init__(self)
        self.message_buffer = message_buffer
        self.prefix_buffer = prefix_buffer

    def run(self):
        logging.debug("Child Thread:Started")

        try:
            mixer = pygame.mixer
            mixer.init()

            if self.prefix_buffer:
                mixer.music.load(io.BytesIO(self.prefix_buffer))
                mixer.music.play()
                mixer.music.queue(io.BytesIO(self.message_buffer))
            else:
                mixer.music.load(io.BytesIO(self.message_buffer))
                mixer.music.play()

            while mixer.music.get_busy():
                time.sleep(0.5)
        except Exception as err:
            logging.error('Failed to play sound stream: %s', str(err))

        logging.debug("Child Thread:Exiting")