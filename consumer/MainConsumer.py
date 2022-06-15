from consumer_sj import Consumer

import multiprocessing
from multiprocessing import Process

def main():
    
    sites = ['SCIENCEON', 'DBPIA', 'NTIS', 'KCI','SCOPUS','WOS']
    processList = []
    for site in sites :

        acl = Consumer(site)

        p = Process(target= acl.run)
        p.start()
        processList.append(p)

    for p in processList :
        p.join()

if __name__ == "__main__":
    main()

    
