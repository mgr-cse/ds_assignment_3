import os
import sys
sys.path.append(os.getcwd())

# raft package
from pysyncobj import SyncObj, SyncObjConf, replicated

# raft object for atm
class KVStorage(SyncObj):
    def __init__(self, selfAddress, partnerAddrs):
        cfg = SyncObjConf(dynamicMembershipChange = True)
        super(KVStorage, self).__init__(selfAddress, partnerAddrs, cfg)
        self.data = {}
    
    @replicated
    def createAccount(self, account: str):
        self.data[account] = 0

    @replicated
    def deposit(self, account: str, value: int):
        self.data[account] += value
    
    @replicated
    def withdraw(self, account, value: int):
        self.data[account] -= value
    
    @replicated
    def transfer(self, account1, account2, value: int):
        self.data[account1] -= value
        self.data[account2] += value

    def inquire(self, account):
        return self.data.get(account, None)
    
    def testAccount(self, account):
        if account in self.data:
            return True
        return False
    
    # for debugging
    def getAccounts(self):
        return self.data

_g_kvstorage = None


def main():
    if len(sys.argv) < 2:
        print('Usage: %s selfHost:port partner1Host:port partner2Host:port ...')
        sys.exit(-1)

    selfAddr = sys.argv[1]
    if selfAddr == 'readonly':
        selfAddr = None
    partners = sys.argv[2:]

    global _g_kvstorage
    _g_kvstorage = KVStorage(selfAddr, partners)

    # interactive shell
    while True:
        cmd = input(">> ").split()
        if not cmd:
            continue
        
        elif cmd[0] == 'create':
            if _g_kvstorage.testAccount(cmd[1]):
                print('account already exists')
            else: _g_kvstorage.createAccount(cmd[1])
        
        elif cmd[0] == 'deposit':
            if _g_kvstorage.testAccount(cmd[1]):
                _g_kvstorage.deposit(cmd[1], int(cmd[2]))
            else: print('account does not exist')
        
        elif cmd[0] == 'withdraw':
            if _g_kvstorage.testAccount(cmd[1]):
                _g_kvstorage.withdraw(cmd[1], int(cmd[2]))
            else: print('account does not exist')
        
        elif cmd[0] == 'transfer':
            if _g_kvstorage.testAccount(cmd[1]) and _g_kvstorage.testAccount(cmd[2]):
                _g_kvstorage.transfer(cmd[1], cmd[2], int(cmd[3]))
            else: print('src or dest account does not exist')
        
        elif cmd[0] == 'inquire':
            if _g_kvstorage.testAccount(cmd[1]):
                print('balance is', _g_kvstorage.inquire(cmd[1]))
            else: print('account does not exist')
        
        elif cmd[0] == 'data':
            print(_g_kvstorage.getAccounts())
        else:
            print('Wrong command')

if __name__ == '__main__':
    main()