def show_nth_label(fname, n):
    f = open(fname, "rU")
    # skip first n-1 images
#    f.seek( (n-1)*img_height )
    for i in range(n-1):
        f.readline()
    label = int(f.readline().rstrip('\n'))
    return label
    
    
if __name__ == '__main__':
    import sys  
    fname = sys.argv[1]
    n = int(sys.argv[2])
    label= show_nth_label( fname, n)
    print(label)

    
    