from django.shortcuts import render
import cv2 as cv
from pyzbar.pyzbar import decode


# Create your views here.
def scanner_view(request):
    return render(request, "index.html", {})
    
def scanned(request):
    id = decode_barcode()
    return render(request, "index.html", {"decoded":id})

def read_bar(filename):
    img = cv.imread(filename)
    detectedBarcodes = decode(img)
    if not detectedBarcodes:
        return "Barcode Not Detected or your barcode is blank/corrupted!"
    else:
        for barcode in detectedBarcodes: 
           
            # Locate the barcode position in image
            (x, y, w, h) = barcode.rect
             
            # Put the rectangle in image using
            # cv2 to heighlight the barcode
            cv.rectangle(img, (x-10, y-10),
                          (x + w+10, y + h+10),
                          (255, 0, 0), 2)
             
            dec = ""
            for i in barcode.data:
                dec += str(chr(i))
            return dec

def decode_barcode():
    cam = cv.VideoCapture(0)
    img_counter = 0
    
    while True:

        ret, frame = cam.read()
        if not ret:
            print("Failed to capture")
            break
        cv.imshow("Scan", frame)
        k = cv.waitKey(1)
        if k%256 == 27:
            cv.destroyAllWindows()
            break
        elif k%256 == 13 or k%256==32:
            cv.imwrite("barcode__.jpg", frame) 
            cv.destroyAllWindows()
            break
    
    return read_bar("barcode__.jpg")