img_height = 28
img_width = 28

def show_nth_image(fname, n):
    f = open(fname, "rU")
    img = []
    # skip first n-1 images
#    f.seek( (n-1)*img_height )
    for i in range( (n-1)*img_height):
        f.readline()
    for i in range(img_height):
        line = f.readline().rstrip('\n')
        img.append(line)
    return img
    
if __name__ == '__main__':
    import sys  
    fname = sys.argv[1]
    n = int(sys.argv[2])
    img = show_nth_image( fname, n)
    str = '\n'.join(img)
    print(str)

    
    