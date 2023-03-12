import tkinter as tk
from tkinter.filedialog import askopenfile
import os
from fscirpt import get_quarters
from fscirpt import main
import xlwt
from tkinter import ttk

def open_file():
    global filepath
    file = tk.filedialog.askopenfile(mode='r',filetypes=[('Excel Files', '*.xlsx')])
    if file:
        filepath = os.path.abspath(file.name)
    print("The File is located at : " + str(filepath))
    filesize = os.path.getsize(file)
    print("The File Size is : " + str(filesize))

root = tk.Tk()
root.title('Stock Scrapper')

global quarters
quarters = []
wb = xlwt.Workbook('Stock Scrapper.xls')

# Helper Nested Functions
def submit():
    url = urlInput.get()
    month = monthVar.get()
    sheetName = SheetInput.get()
    dateval = datevar.get()
    print("URL : ", url)
    print("\nProcessing Quarter : ", month)
    print("\nSHEETNAME : ", sheetName)
    main(filepath, sheetName, month,dateval)
    print(month + " " + "Quarter data scrapped successfully")

def fill_drop():
    quarters = get_quarters(urlInput.get())
    monthDrop = tk.OptionMenu(root, monthVar, *quarters)
    monthDrop.place(x=50, y=150)

#url
urlText = tk.Label(root,text="URL",bg="black",fg="white",font=("American Typewriter", 14))
urlText.place(x=50, y=30)
urlInput = tk.Entry(root, width=70)
urlInput.place(x=50, y=70)
urlInput.insert(0,"https://www.moneycontrol.com/financials/landmarkcars/results/quarterly-results/LC07#LC07")

#month
monthVar = tk.StringVar()
monthText = tk.Label(root,text="SELECT THE MONTH : ",bg="black",fg="white",font=("American Typewriter", 14))
monthText.place(x=50, y=110)
# monthDrop = tk.OptionMenu(root, monthVar, [quarters])
# monthDrop.place(x=50, y=150)
# OptionType = tk.Entry(root, width=30)
# OptionType.place(x=250, y=150)


#reset monthdrop
# monthVar.set("")
# monthVar.set("Select Month")

#sheetname
SheetName = tk.Label(root,text="SHEET NAME",bg="black",fg="white",font=("American Typewriter", 14))
SheetName.place(x=50, y=200)
SheetInput = tk.Entry(root, width=50)
SheetInput.place(x=50, y=250)
SheetInput.insert(0,"Qtr_res_out")

#buttons
confirm_button = tk.Button(root, text="Load URL", command=fill_drop)
confirm_button.place(x=550, y=70)
submit_button = tk.Button(root, text="Submit", command=submit)
submit_button.place(x=300, y=300)
uploadButton = tk.Button(root, text="Browse Input file", command=open_file)
uploadButton.place(x=50, y=300)

#drop menu with options yes and no
datevar = tk.StringVar()
Datedrop = tk.OptionMenu(root, datevar, "NO", "YES")
Datedrop.place(x=50, y=500)
datevar.set("")
datevar.set("Ignore Result Dates")

root.geometry("800x800")
root.resizable(False, False)
root.configure(background='black')
root.mainloop()
