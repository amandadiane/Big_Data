import os, sys
    
n = int(sys.argv[1])

fname = 'train_images.txt'
cmd = "python nth_image.py {} {}".format( fname, n)
os.system( cmd )

input()

fname = 'train_labels.txt'
cmd = "python nth_label.py {} {}".format( fname, n)
os.system( cmd )

