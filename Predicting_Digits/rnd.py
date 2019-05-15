import os, sys, random
    
for i in range(100):
    n = random.randint(1,5000)

    fname = 'train_images.txt'
    cmd = "python nth_image.py {} {}".format( fname, n)
    os.system( cmd )

    input()

    fname = 'train_labels.txt'
    cmd = "python nth_label.py {} {}".format( fname, n)
    os.system( cmd )

    input()


