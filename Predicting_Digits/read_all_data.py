IMAGE_HEIGHT = 28
IMAGE_WIDTH = 28

#################################################
# read_all_image_label
#################################################

def read_all_image_label(img_file='test_images.txt', \
                         lbl_file='test_labels.txt'):
    fp_img = open(img_file, 'r')
    fp_lbl = open(lbl_file, 'r')
    all_img = []
    all_lbl = []
    try:
        while True:
            img = []
            for j in range(IMAGE_HEIGHT):
                line = fp_img.readline().rstrip('\n')
                img.append(line)
            lbl = int(fp_lbl.readline().rstrip('\n'))
            all_img.append(img)
            all_lbl.append(lbl)
    except (EOFError, ValueError):
        fp_img.close()
        fp_lbl.close()
    finally:
        return( all_img, all_lbl )

def show_matrix(m):
    for i in range(10):
        for j in range(10):
            str = "{:6.1f}".format( m[i][j]*100)
            print(str, end='')
        print()



def test():
    train_img, train_lbl = read_all_image_label('train_images.txt', 'train_labels.txt')
    test_img,  test_lbl  = read_all_image_label('test_images.txt', 'test_labels.txt')
    print("# of training images: %d" % len(train_img))
    print("# of training labels: %d" % len(train_img))

    print("# of test images:     %d" % len(test_img))
    print("# of test labels:     %d" % len(test_img))

# if __name__ == '__main__':
#     test()
