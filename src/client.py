from cmd import Cmd
from querymanager import proc_main

class TPSQLClient(Cmd):
    intro ='Welcome to TPSQL. Please enter your query!'
    prompt = 'TPSQL > '
    
    def do_exit(self, inp:str) -> bool:
        ''' Exit from TPSQL client'''
        print('Bye... Shutting down TPSQL')
        return True
    
    def do_select(self, inp:str)  -> None:
        '''  Command should start with SELECT ...'''
        if len(inp.strip()) >= 1:
            # send to query manager
            try:
                res = proc_main('select '+inp)
            except Exception as e:
                print('Exception Occurred: See logs for details: \n'+str(e))
            if res == True:
                print('Query Execution Completed..')
            else:
                print('Problem in Execution... Please see logs for errors')
        else:
            print('*** No input provided')
    
    def help_select(self):
        print('Send a select SQL query to the TPSQL service')
    
    def default(self, inp):
        if inp == 'x' or inp == 'q':
            return self.do_exit(inp)

    do_EOF = do_exit

if __name__ == "__main__":
    TPSQLClient().cmdloop()