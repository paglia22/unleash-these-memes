#from imageai.Detection import ObjectDetection
import os
import cv2
import re
import enchant
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
os.environ['TESSDATA_PREFIX'] = '/usr/local/Cellar/tesseract/4.1.1/share/tessdata/'

try:
    from PIL import Image
except ImportError:
    import Image
import pytesseract

def text_preprocessing(text):
    text = text.lower()  # Put in lowercase
    text = text.replace("\n", " ")  # Remove new lines from the text
    text = re.sub("[!”#$%¥&’()©*‘+\[,-.¢“/@\":;<=>™?^_—`{®|}~«»%]", "", text)  # Remove symbols
    text = re.sub(r'[0-9]', '', text)  # Remove numbers (digits)
    text = re.sub(" +", " ", text)  # Remove multiple white spaces
    return text

my_brand_list_full = ["gucci", "netflix", "colgate", "alexa", "gamestop", "lg", "iphone", "oreo", "walmart", "chipotle", "mcdonald", "astrazeneca", "uber eats", "coca cola", "michelin", "nike", "doritos", "tesla", "volkswagen", "xbox", "ford", "Disneyland", "amazon fire", "amd", "blackberry", "apple", "intel", "nokia", "samsung", "nintendo", "android", "burger king", "kfc", "taco bell", "wendy", "five guys", "starbucks", "vine", "apple music", "amazon prime", "wikipedia", "blockbuster", "spotify", "microsoft", "EA", "mountain dew", "diet coke", "adidas", "pringles", "audi", "bmw", "playstation"]
def ocr_meme(filename):
    img = cv2.imread(filename)
    img = cv2.bilateralFilter(img, 5, 55, 60) # Bilateral Filter
    img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY) # Convert into greyscale
    _, img1 = cv2.threshold(img, 240, 255, 0)  # Helps in finding BLACK text: if the pixel value is smaller than the threshold (240), then it is set to 0, otherwise it is set to 255

    custom_config = "--oem 3 --psm 11" #Optimize configuration, "--psm 11" means that we look for sparse text

    text_1 = pytesseract.image_to_string(img1, config=custom_config) # We'll use Pillow's Image class to open the image and pytesseract to detect the string in the image

    text_1 = text_preprocessing(text_1) # Clean the text

    d = enchant.Dict("en_US")
    count_1 = 0
    new_text = ""
    for word in text_1.split():
        if d.check(word) == True or word in my_brand_list_full: # Verify if the detected word is a real word contained in the English dictionary
            count_1 += 1 # Count how many real words have been detected
            new_text = new_text + word + " " # Add the word in a new
    text_1 = new_text[:-1] # Remove the last character, which is a white space, from the new string

    # SECOND OCR. Should be more effective with WHITE text:
    img = cv2.bitwise_not(img)
    _, img = cv2.threshold(img, 240, 255, 1)  # Helps in finding WHITE text (common in memes): if the pixel value is smaller than the threshold (240), then it is set to 0, otherwise it is set to 255. Then, the colors are inverted

    text_2 = pytesseract.image_to_string(img, config=custom_config)  # We'll use Pillow's Image class to open the image and pytesseract to detect the string in the image
    text_2 = text_preprocessing(text_2)

    count_2 = 0
    new_text = ""
    for word in text_2.split():
        if d.check(word) == True:  # Verify if the detected word is a real word contained in the English dictionary
            count_2 += 1  # Count how many real words have been detected
            new_text = new_text + word + " "  # Add the word in a new text
    text_2 = new_text[:-1]  # Remove the last character, which is a white space, from the new string

    if count_1 >= count_2:
        return text_1
    else:
        return text_2

def read_backup():
    a_file = open("/Users/paglia/Desktop/ProveTesi/obj/OCR_backup.txt", "r")
    list = []
    for line in a_file:
      stripped_line = line.strip()
      list.append(stripped_line)
    a_file.close()
    return list

def execute_ocr_on_dataset():
    df = pd.read_csv("/Users/paglia/Desktop/ProveTesi/memes_dataset_final.csv")

    backup_list = read_backup()
    len_list = len(backup_list)

    directory = "/Users/paglia/Desktop/ProveTesi/images"
    text_lst = []

    text_lst = backup_list

    iteration = 1
    backup = 0
    nrows = df.shape[0]
    for index, row in df.iterrows():
        if iteration <= len_list:
            iteration += 1
            continue

        print("Executing " + str(iteration) + "/" + str(nrows))
        iteration += 1
        backup += 1
        pos = str(row["url"]).rfind(".")
        img_type = str(row["url"])[pos:]  # Get image type (file format: jpg, png, gif...
        filename = str(row["brandname"]) + "_" + str(row["id"]) + str(img_type)

        if filename.endswith("png") or filename.endswith("jpg"):
            img_path = directory + "/" + filename
            text_lst.append(ocr_meme(img_path))
        else:
            text_lst.append("")
        if backup == 1000:
            textfile = open("OCR_backup.txt", "w")
            for e in text_lst:
                textfile.write(e + "\n")
            textfile.close()
            backup = 0
    df["text_ocr"] = text_lst
    df.to_csv("OCR.csv")

#execute_ocr_on_dataset()
#print(ocr_meme("/Users/paglia/Desktop/ProveTesi/images/apple_hemoiu.png"))






def imageai_detection():
    execution_path = os.getcwd()

    detector = ObjectDetection()
    detector.setModelTypeAsRetinaNet()
    detector.setModelPath( os.path.join(execution_path , "resnet50_coco_best_v2.1.0.h5"))
    detector.loadModel()
    detections = detector.detectObjectsFromImage(input_image=os.path.join(execution_path , "image.jpeg"), output_image_path=os.path.join(execution_path , "imagenew.jpg"))

    for eachObject in detections:
        print(eachObject["name"] , " : " , eachObject["percentage_probability"] )
