# p2p
import argparse
from socket import *
import threading
import pathlib
from flask.globals import request
import glob, os
import random


#ipTable = {0:{'ip': '127.0.0.1', 'port': 8001},1:{ 'ip': '127.0.0.1', 'port': 8002},2:{ 'ip': '127.0.0.1', 'port': 8003},3:{ 'ip': '127.0.0.1', 'port': 8004}}
fileTable = {'127.0.0.1:8001': '','127.0.0.1:8002': '','127.0.0.1:8003': '','127.0.0.1:8004': '' }
ipTable = {0:{'ip': '127.0.0.1', 'port': 8001},1:{ 'ip': '127.0.0.1', 'port': 8002},2:{ 'ip': '127.0.0.1', 'port': 8003},3:{ 'ip': '127.0.0.1', 'port': 8004}}



class P2PClient():
    def __init__(self, ip,port, choice):
        self.ip = ip
        self.port = port
        self.file = '1.pdf'
        if choice == 1:
            self.getInput()
        if choice == 2:
            self.checkLog()
       

    def Client_file(self, filename, ip, port, b,s, e, logName):
        client_socket = socket(AF_INET, SOCK_STREAM)
        client_socket.settimeout(2)
        try:
            client_socket.connect((ip,port))
            #client_socket.shutdown(2)
        except :#Exception as msg:
            #client_socket.close()
            print('{0} at {1} is not open, so the file {2} cannot be downloaded from there.\nBroadcasting again\n'.format(ip,port, filename))
            self.broadcast(filename)
        # b s e
        start = b
        if s == 0:
            start = s
        elif s>0 and s<e:
            start = s
        else:
            return
        message = 'GET '+ filename + ' '+str(start)+ ' '+str(e)+'\r\n\r\n'
        
        client_socket.send(message.encode())
        path = pathlib.Path(filename)
        length = 0
        if path.exists() and os.stat(filename).st_size == e:
            print("File exists")
        elif path.exists() and os.stat(filename).st_size < e:
            with open(filename, 'ab') as f:
                 print ('Getting {0} from {1} at {2}\n'.format(filename, ip, port))
                 while True:
                    try:
                        data = client_socket.recv(1024)
                        if not data:
                            break
                        f.write(data)
                        length += len(data)
                    except:
                        print("lost connection: {0} at {1}".format(ip,port))
                        break
            print('Total ',length,' Bytes Got\n')
            f.close()
        else:
            length = 0
            with open(filename, 'wb') as f:
                 print ('Getting {0} from {1} at {2}\n'.format(filename, ip, port))
                 while True:
                    try:
                        data = client_socket.recv(1024)
                        if not data:
                            break
                        f.write(data)
                        length += len(data)
                    except:
                        print("lost connection: {0} at {1}".format(ip,port))
                        break
            print('Total ',length,' Bytes Got\n')
            f.close()
            
        with open(logName, 'r') as f:
            lines = f.readlines()
        f.close()
        
        
        for i in range(len(lines)):
            if filename in lines[i]:
                l = int(lines[i].split(' ')[3])
                lines[i] = filename + ' ' + str(0) + ' ' + str(e) + ' '+ str(l+length)+' '+ ip +':'+str(port)+'\n'
        
#         for line in lines:
#             if filename in line:
#                 l = int(line.split(' ')[3])
#                 line = filename + ' ' + str(s) + ' ' + str(e) + ' '+ str(l+length)+' '+ str(ip)+':'+str(port)+'\n'
#         print(lines)

        with open(logName, 'w') as f:
            f.writelines(lines)
        f.close()
        
        client_socket.close()
    def checkLog(self):
        # os.chdir(os.getcwd())
        # for log in glob.glob("*.log"):
        self.get_request_from_log('file.log')




    def getInput(self):
        #file = input('Enter the file name: \n')
        file = self.file
        if file =='':
            return None
        file2 = self.file
        file2 = file2.split(' ')
        print(file2)
        path = pathlib.Path('file.log')
        if path.exists():
            for name in file2:
                with open('file.log', 'r') as f:
                    lines = f.readlines()
                f.close()
                for line in lines:
                    if name in line:
                        file = file.replace(name,"")
                        print('file {0} exists'.format(name))
        
        print(file)
        if file =='':
            return 
        self.broadcast(file)


    def broadcast(self, file):
        
        message = 'WHERE {0}\r\n\r\n'.format(file)
        #print(message)
        i = 0
        response=''
        for i in range(4):
            server_host = ipTable[i]['ip']
            server_port = ipTable[i]['port']
            flag= 1
            #print(server_host, server_port)
            if str(server_host) == str(self.ip) and int(server_port) == int(self.port):
                continue
            client_socket = socket(AF_INET, SOCK_STREAM)
            client_socket.settimeout(2)
            try: 
                client_socket.connect((server_host,server_port))
                #client_socket.settimeout(2) 
            except:
                print("Cannot connect to {0} at {1}".format(server_host, server_port))
                flag = 0

            if flag == 1:
                client_socket.send(message.encode())
                response += client_socket.recv(1024).decode() + server_host + ':' + str(server_port) + '\r\n\r\n'
            client_socket.close()

        if response is not '':
            print(response)
            self.process_response_save_to_log(response, file)
        return response
    
    def process_response_save_to_log(self, response, file):
        logName = 'file.log'
        response = response.split('\r\n\r\n')
        file = file.split(' ')
        #print(file)
        fileList = []
        for c in response:
            c = c.split('\r\n')
            for f in c:
                if 'HERE' in f:
                    fileList.append(f+'@'+c[-1])
        print(fileList)

        file2 = []
        for item in fileList:
            if 'HERE' in item:
                file2.append(item.split(' ')[1])
        file2 = list(set(file2))
        print(file2, len(file2))


        requestList =[]
        for f in file2:
            pick = random.choice(fileList)
            while 1:
                pick = random.choice(fileList)
                if f in pick :
                    pick = pick[5:]
                    requestList.append(pick)
                    break
        print(requestList)

        with open(logName, 'a') as f:
            for pair in requestList:
                file = pair.split(" ")[0]
                size = pair.split("@")[0].split(" ")[-1].replace('\n', "")
                host = pair.split("@")[1]
                print(file, size)
                if not self.findSame(file,host):
                    state = file + ' 0 '+str(size)+' 0 '+ host+'\n'#here
                    f.write(state)
        f.close()
        #self.process_log(logName)
        self.get_request_from_log(logName)
        return logName

    def findSame(self, file, host):
        with open('file.log', 'r') as f:
            lines = f.readlines()
        f.close()
        flag = 0
        for i in range(len(lines)):
            if file in lines[i]:
                filename1 = lines[i].split(' ')[0]
                end1 = lines[i].split(' ')[2]
                start1 = lines[i].split(' ')[3]
                #host1 = lines[i].split(' ')[-1].split(':')
                lines[i] = file + ' 0 '+str(end1)+ ' '+str(start1) + ' '+ host+'\n'#here
                flag = 1
        lines = list(set(lines))
        if flag == 1:
            with open('file.log', 'w') as f:
                f.writelines(lines)
            f.close()

        return flag

    def get_request_from_log(self, logName):
        with open(logName, 'r') as f:
            lines = f.readlines()
        f.close()
        count = 0
        threadList = []
        for line in lines:
            if line == '\n' or line =='':
                continue
            host = line.split(' ')[-1].split(':')
            filename = line.split(' ')[0]
            begin = int(line.split(' ')[1])
            end = int(line.split(' ')[2])
            start = int(line.split(' ')[3])
            print(int(host[1]), filename, begin, end, start)

            if start == end :
                print('Nothing new needed to be downloaded\n')
                continue
            t = threading.Thread(target=self.Client_file, args=(filename, host[0], int(host[1]), begin,start, end, logName))
            t.start()
            print('Thread {0} started to download {1}....'.format(count, filename))
            count+=1
            threadList.append(t)


        for item in threadList:
            item.join()
        self.updatelog()

    def updatelog(self):
        # os.chdir(os.getcwd())
        # for log in glob.glob("*.log"):
            with open('file.log', 'r') as f:
                lines = f.readlines()
            f.close()
            #print(lines)
            names = []
            count = 0
            for i in range(len(lines)):
                if lines[i] == '' or lines[i] == '\n':
                    continue
                host = lines[i].split(' ')[-1]
                filename = lines[i].split(' ')[0]
                end = lines[i].split(' ')[2]
                start = lines[i].split(' ')[3]
                size = os.stat(filename).st_size
                if int(start) != int(size):
                    lines[i] = filename + ' 0 '+str(end)+ ' '+str(size) + ' '+ host+'\n'
                if int(start) == int(end):
                    # try:
                    #     with open('1.pdf.log', 'r') as f:
                    #         lines = f.readlines()
                    #     f.close()
                    # except:
                    #     print("Log for 1.pdf is build")
                    #     f = open('1.pdf.log', 'w')
                    #     f.close()
                    # f = open('1.pdf.log', 'a')
                    # for line in lines:
                    #     if filename not in line:
                    #         f.write(filename +' '+str(start)+'\n')
                    names.append(int(filename.split('_')[1]))
                    count += 1
                    print(names, count)
                    
            with open('file.log', 'w') as f:
                 f.writelines(lines)
            f.close()

            if count == 5:
                print(names)
                names.sort()
                print(names)
                # with open('file.log', 'r') as f:
                #     lines = f.readlines()
                # f.close()
                # with open('1.pdf.log', 'w') as f:
                #     for i in range(len(lines)):
                #         filename = lines[i].split(' ')[0]
                #         start = lines[i].split(' ')[3]
                #         f.write(filename + ' '+start+'\n')
                # f.close()
                fileString = ''
                for i in names:
                    fileString = fileString + 'split_'+str(i)+ ' ' 

                os.system("cat "+fileString+'>> 1.pdf')
                for i in names:
                    fileString = 'split_'+str(i)
                    os.system("rm "+fileString)
                os.system("rm 1.pdf.log")
                os.system("rm file.log")





# WHERE filename\r\n\r\n
# GET filename IP PORT x y\r\n\r\n
# SEND filename x y\r\n\r\ndata
# HERE filename IP PORT x y\r\n\r\n

# server part

class clientThread(threading.Thread):
    def __init__(self, csocket,caddr ):
        threading.Thread.__init__(self)
        self.s = csocket
        self.a = caddr
        self.split_first()

    
        

    def split_first(self):

        namebase = 'split_'

        totalsize = os.stat('1.pdf').st_size

        chunksize = 500000 # 2350755/4 = 587688.75

        reminder = totalsize % chunksize

        print('chunksize is {0}, reminder is {1}'.format(chunksize,reminder))

        with open('1.pdf', 'rb') as f1:
            with open('1.pdf.log', 'w') as f2:
                for i in range(5):
                    if i == 4:
                        data = f1.read()
                    else:
                        data = f1.read(chunksize)
                    #print('lenth is ',len(data))
                    f = open(namebase+str(i), 'wb')
                    f.write(data)
                    f.close()
                    message = namebase+str(i)+' '+str(len(data))+'\n'
                    f2.write(message)
            f2.close()
        f1.close()

    def run(self):
        request = self.s.recv(1024).decode().split('\r\n\r\n')[0]
        request = request.split(" ")
        if request[0] == 'WHERE':
            self.findFilesAndResponse(request)
        if request[0] == 'GET':
            self.sendFile(request)
        else: 
            return
        self.s.close()
        
    def sendFile(self, request):
        print(request)

        fileName = request[1]
        start = request[2]
        end = request[3]
        with open(fileName, 'rb') as f:
            drop = f.read(int(start)) # drop the first s bytes
            print ('Sending {0} to {1}'.format(fileName, self.a))
            length = 0
            while True:
                data = f.read(1024)
                if not data:
                    break
                self.s.send(data)
                length += len(data)
        f.close()
        print('Total ', length, ' bytes sent\n')
        
        

    def findFilesAndResponse(self, request):
        response = ''
        for item in request:
            if len(item) == 0:
                request.remove(item) 
        print(request)
        for file in request[1:]:
            # if file == '':
            #     continue
            filelog = '1.pdf.log' # file +'.log'
            path = pathlib.Path(filelog)
            if path.exists():
                try:
                    f = open(filelog, 'r')
                    lines = f.readlines()
                    f.close()
                    for line in lines:

                        file2 = line.split(' ')[0]
                        size = line.split(' ')[1]
                        if file == '1.pdf':
                            response += 'HERE ' + file2 +' ' +size+ '\r\n'
                        else:
                            if file == file2:
                                response += 'HERE ' + file2 +' ' +size+ '\r\n'


                except:
                    continue
            else:
                continue
        if response == '':
            response = 'NO\r\n'
        self.s.send(response.encode())


class P2PServer():
    def __init__(self, ip,port ):
        self.ip = ip
        self.port = port
        self.runServer()

    def clean(self):
        print('cleaning.......')
        path = pathlib.Path('1.pdf')
        if path.exists():
            for i in range(5):
                fileString = 'split_'+str(i)
                os.system("rm "+fileString)
            os.system("rm 1.pdf.log")
    
    def runServer(self):
        server_socket = socket(AF_INET, SOCK_STREAM)
        server_socket.bind((self.ip, self.port))
        server_socket.listen(1)
        while True:
            print('P2P Server {0} at {1} is now ready to serve...'.format(self.ip, self.port))
            connectionSocket, addr = server_socket.accept()
            print('Received a connection from:', addr)
            newThread = clientThread(connectionSocket, addr)
            newThread.start()
            self.clean()
        server_socket.close()





if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='python client.py --ip CLIENT_CLIENT_IP --port CLIENT_PORT')
    parser.add_argument('--port', type=int, dest='port', help='Port # for client')
    parser.add_argument('--ip', dest='ip',help='IP address for Client')
    args = parser.parse_args()


    a = threading.Thread(target=P2PServer, args=(args.ip, args.port))
    #b = threading.Thread(target=P2PClient, args=(args.ip, args.port, 1))
   
    a.start()
    #b.start()

    # client = P2PClient(args.ip, args.port, '1.pdf')
    # server = P2PServer(args.ip, args.port)
    filelog = 'file.log'
    path = pathlib.Path(filelog)
    if not path.exists():
        i = int(input('Press 1 to know who has the file you need\nPress -1 to exit\n'))
        while i != -1:
            if i == -1:
                break
            if i == 1:
                print('downloading 1.pdf...')
                b = threading.Thread(target=P2PClient, args=(args.ip, args.port, i))
                b.start()
            i = int(input('Press 1 to know who has the file you need\nPress -1 to exit\n'))
    else:
        i = int(input('Press 1 to redownload all chunks\nPress 2 to finish downloading\nPress -1 to exit\n'))
        #client.updatelog()
        while i != -1:

            if i == -1:
                break

            if i == 1:
                print('downloading 1.pdf...')
                b = threading.Thread(target=P2PClient, args=(args.ip, args.port, i))
                b.start()
           
            if i == 2:
                b = threading.Thread(target=P2PClient, args=(args.ip, args.port, i))
                b.start()
                #start.updatelog() 
            i = int(input('Press 1 to redownload all chunks\nPress 2 to finish downloading\nPress -1 to exit\n'))

    
    
    
    
    
    
    
    
    
    
    
