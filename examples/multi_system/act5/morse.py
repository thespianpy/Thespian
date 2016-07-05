from thespian.actors import requireCapability
from encoder import Encoder

@requireCapability('morse')
class MorseEncoder(Encoder):
    def encode(self, rawstr):
        return ' '.join(filter(None, [self.morsechar(C) for C in rawstr]))
    @staticmethod
    def morsechar(char):
        return { 'A': '.-',
                 'B': '-...',
                 'C': '-.-.',
                 'D': '-..',
                 'E': '.',
                 'F': '..-.',
                 'G': '--.',
                 'H': '....',
                 'I': '..',
                 'J': '.---',
                 'K': '-.-.',
                 'L': '.-..',
                 'M': '--',
                 'N': '-.',
                 'O': '---',
                 'P': '.--.',
                 'Q': '--.-',
                 'R': '.-.',
                 'S': '...',
                 'T': '-',
                 'U': '..-',
                 'V': '...-',
                 'W': '.--',
                 'X': '-..-',
                 'Y': '-.--',
                 'Z': '--..',
                 '0': '-----',
                 '1': '.----',
                 '2': '..---',
                 '3': '...--',
                 '4': '....-',
                 '5': '.....',
                 '6': '-....',
                 '7': '--...',
                 '8': '---..',
                 '9': '----.',
                 ' ': '    ',
        }.get(char.upper(), None)

