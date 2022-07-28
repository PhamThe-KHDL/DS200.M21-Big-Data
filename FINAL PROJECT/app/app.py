# from Tkinter import *
# import datetime

# root = Tk()

# lab = Label(root)
# lab.pack()
# a = 1


# root.mainloop()
# from PIL import ImageTk, Image
from Tkinter import *
import datetime
import Tkinter 
import pandas as pd  
import numpy as np 

data = pd.read_csv('C:\kafka-demo\data_final\display.csv')

data.columns = ['ID' ,'QUARTER' ,'MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', 
                    'OP_UNIQUE_CARRIER', 
                    'ORIGIN',
                    'DEST','DISTANCE',
                    'CRS_DEP_TIME', 
                    'LABEL','prediction']
def doiso(df): 
    for i in range(len(df)):
        if df['prediction'][i] == 0.0:
             df['prediction'][i] = "Not Delay"
        if df['prediction'][i] == 1.0:
             df['prediction'][i] = "Delay 30m"
        if df['prediction'][i] == 2.0:
             df['prediction'][i] = "Delay more than 30m"
doiso(data)
window = Tk()
window.title("App")
window.configure()
lbl = Label(window, text= "Departures", font= ("Arial", 20))



lbl.grid(column = 0, row = 0)
time = datetime.datetime.now().strftime("Time: %H:%M:%S")
lbl2 = Label(window, text= str(time), font= ("Arial", 20))
lbl2.grid(column = 4, row = 0)
# lbl.place(x=0, y=0)
color = ['cyan', 'black']
color_f = ['black','white']
# for i in range(5):
#     for j in range(6):
#         # if i % 2 == 0:
#         #     bg = 'color'
#         # l = Label(text='%d.%d' % (i, j), relief=RIDGE, bg=color[i%2], fg=color_f[i%2], pady=15, padx=50, font=10)
#         l = Label(text=''+str(data.iloc[:,0][i]+''), relief=RIDGE, bg=color[i%2], fg=color_f[i%2], pady=15, padx=50, font=10)
#         l.grid(row=i+1, column=j, sticky=NSEW)


def clock():
    data = pd.read_csv('C:\kafka-demo\data_final\display.csv')
    
    doiso(data)
    time = datetime.datetime.now().strftime("Time: %H:%M:%S")
    # lab.config(text=str(b))
    # lab['text'] = time
    # b = b + 1
     # run itself again after 1000 ms
    
    lbl = Label(window, text= "Chuyen Bay/Departures", font= ("Arial", 20))
    lbl.grid(column = 0, row = 0)
    # lbl1 = Label(window, text= "Departures", font= ("Arial", 20))
    # lbl1.grid(column = 1, row = 0)
    
    time = datetime.datetime.now().strftime("Time: %H:%M:%S")
    lbl2 = Label(window, text= str(time), font= ("Arial", 20))
    lbl2.grid(column = 4, row = 0)
    
    # img = ImageTk.PhotoImage(Image.open('C:\kafka-demo\figture\maybay.jpg'))
    # panel = Label(window, image = img)
    # panel.pack(side = "bottom", fill = "both", expand = "yes")
    # lbl.place(x=0, y=0)
    color = ['cyan', 'black']
    color_f = ['black','white']
    for i in range(1):
        for j in range(5):
            # l = Label(text=''+str(data.iloc[:,11][i]+''), relief=RIDGE, bg=color[i%2], fg=color_f[i%2], pady=15, padx=50, font=10)
            if(j == 0):
                l = Label(text=''+str(data.columns[0]+''), relief=RIDGE, bg=color[i%2], fg=color_f[i%2], pady=15, padx=50, font=10)
            elif (j==1): 
                l = Label(text=''+str(data.columns[6]+''), relief=RIDGE, bg=color[i%2], fg=color_f[i%2], pady=15, padx=50, font=10)
            elif (j==2): 
                l = Label(text=''+str(data.columns[7]+''), relief=RIDGE, bg=color[i%2], fg=color_f[i%2], pady=15, padx=50, font=10)
            elif (j==3): 
                l = Label(text=str(data.columns[9]+''), relief=RIDGE, bg=color[i%2], fg=color_f[i%2], pady=15, padx=50, font=10)
                # l = Label(text='%d.%d' % (i, j), relief=RIDGE, bg=color[i%2], fg=color_f[i%2], pady=15, padx=50, font=10)
            elif (j==4): 
                l = Label(text=''+str(data.columns[11]+''), relief=RIDGE, bg=color[i%2], fg=color_f[i%2], pady=15, padx=50, font=10)
            l.grid(row=i+1, column=j, sticky=NSEW)
    for i in range(9):
        for j in range(5):
            # if i % 2 == 0:
            #     bg = 'color'
            # l = Label(text='%d.%d' % (i, j), relief=RIDGE, bg=color[i%2], fg=color_f[i%2], pady=15, padx=50, font=10)
            if(j == 0):
                l = Label(text=''+str(data.iloc[:,0][i]+''), relief=RIDGE, bg=color[i%2], fg=color_f[i%2], pady=15, padx=50, font=10)
            elif (j==1): 
                l = Label(text=''+str(data.iloc[:,6][i]+''), relief=RIDGE, bg=color[i%2], fg=color_f[i%2], pady=15, padx=50, font=10)
            elif (j==2): 
                l = Label(text=''+str(data.iloc[:,7][i]+''), relief=RIDGE, bg=color[i%2], fg=color_f[i%2], pady=15, padx=50, font=10)
            elif (j==3): 
                l = Label(text=str(data.iloc[:,9][i]), relief=RIDGE, bg=color[i%2], fg=color_f[i%2], pady=15, padx=50, font=10)
                # l = Label(text='%d.%d' % (i, j), relief=RIDGE, bg=color[i%2], fg=color_f[i%2], pady=15, padx=50, font=10)
            elif (j==4): 
                l = Label(text=''+str(data.iloc[:,11][i]+''), relief=RIDGE, bg=color[i%2], fg=color_f[i%2], pady=15, padx=50, font=10)
            l.grid(row=i+2, column=j, sticky=NSEW)
    window.after(15000, clock)
# run first time
clock()

mainloop()