import pyautogui
import pygetwindow as gw
import time
import os
from io import open
from jamo import h2j, j2hcj

def isHangul(text):
    import sys
    import re
    #Check the Python Version
    pyVer3 =  sys.version_info >= (3, 0)

    if pyVer3 : # for Ver 3 or later
        encText = text
    else: # for Ver 2.x
        if type(text) is not unicode:
            encText = text.decode('utf-8')
        else:
            encText = text

    hanCount = len(re.findall(u'[\u3130-\u318F\uAC00-\uD7A3]+', encText))
    return hanCount > 0

def isHanguelToEng(string):
    keyMapping_eng = ("y", "u", "i", "o", "O", "p", "P", "h", "j", "k", "l", "b", "n", "m", "q", "Q", "w", "W", "e", "E", "r", "R", "t", "T", "a", "s", "d", "f", "g", "z", "x", "c", "v" )
    keyMapping_kor = ("ㅛ", "ㅕ", "ㅑ", "ㅐ", "ㅒ", "ㅔ", "ㅖ", "ㅗ", "ㅓ", "ㅏ", "ㅣ", "ㅠ", "ㅜ", "ㅡ", "ㅂ", "ㅃ", "ㅈ", "ㅉ", "ㄷ", "ㄸ", "ㄱ", "ㄲ", "ㅅ", "ㅆ", "ㅁ", "ㄴ", "ㅇ", "ㄹ", "ㅎ", "ㅋ", "ㅌ", "ㅊ", "ㅍ")
    return_list = []
    for char in string :
        indexNum = keyMapping_kor.index(char)
        return_list.append(keyMapping_eng[indexNum])
    return return_list

win = gw.getWindowsWithTitle('Chrome')[0]

os_path = os.getcwd()

file_list = os.listdir(os_path)

win = gw.getWindowsWithTitle('Chrome')[0]

win.activate() # 해당 윈도우를 활성화

for file in file_list :
    root,extension = os.path.splitext(file)

    # print(root)
    # print(extension)

    if extension == '.txt':
          # print(file)
        with open(file,'r', encoding='utf-8') as file_object:
            contents = file_object.read()

            for char in contents :
                if isHangul(char) :
                    pyautogui.press('hanguel')
                    temp_char = j2hcj(h2j(char))
                    press_list = isHanguelToEng(temp_char)

                    for char in press_list :
                        pyautogui.typewrite(str(char))
                        time.sleep(0.05)
                    # pyperclip.copy(char)
                    # # 클립보드에 있는 내용을 붙여넣기
                    # pyautogui.hotkey("ctrl", "v")
                    pyautogui.press('hanguel')

                else:
                    pyautogui.typewrite(str(char))
                    time.sleep(0.05)