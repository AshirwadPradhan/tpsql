from cmd import Cmd

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
            print('send the sql query to querymanager')
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