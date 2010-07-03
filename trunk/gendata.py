import math, random, sys, getopt

def gen_random_str(str_len, alphabet="abcdefghijklmnopqrstuvwxyz1234567890"):
    """
    generates a random string of length (str_len)
    """
    alpha_inds = map(lambda x: int(random.uniform(-1,len(alphabet)-1)), range(str_len))
    return "".join(map(lambda ai: alphabet[ai], alpha_inds))

def coin_toss(p=0.5):
    """
    return head (True) with probability p 
    """
    r = random.uniform(0,1)
    if(r<=p):
        return True
    else:
        return False

def uniform_die_toss(nfaces):
    """
    Samples from a fair "die toss" (multinomial) with each face having
    equal probability
    """
    return int(math.floor(random.uniform(0,nfaces)))

def usage():
    #prints usage message
    print "--------> python gendata.py [OPTION]"
    print "--------> options: -n[number of lines] . Defaults to 10000"
    print "-------->          -p[percent of real lines] . Defaults to 0.5"
    print "--------> example usage:"
    print "-------->               gendata -n100 -p0.25"


def gendata(N, p, real_path="data/biz.dat", output_file=sys.stdout):
    """
    Generates N lines.
    """
    #read in real data (already stored in database hopefully)
    realdataf = open(real_path) 
    #read in lines (stripped of white space)
    lines = [l.strip() for l in realdataf]
    realdataf.close()        
    for itr in range(N):
        #decide on whether to choose real or fake data
        if coin_toss(p):
            #real
            output_file.write( lines[uniform_die_toss(len(lines))]  + "\n")
        else:
            #fake
            rbname = gen_random_str(int(random.gauss(10,3)))
            rburl  = gen_random_str(int(random.gauss(20,4)))
            output_file.write( rbname + '\t' + rburl + "\n")




if __name__ == "__main__":
    N = 10000; p = 0.5 #default values
    #process inputs
    try:
        opts, args = getopt.getopt(sys.argv[1:], "n:p:")
    except getopt.GetoptError, err:
        print str(err)
        usage()
        sys.exit(-1)

    #processes inputs
    try:
        for o,a in opts:
            if o == "-n":
                N = int(a)
            if o == "-p":
                p = float(a)
                if not(p>=0 and p<=1.0):
                    raise Exception("invalid p value. Must be >=0 and <=1")
    except Exception:
        usage()
        sys.exit(-1)

    gendata(N,p)

