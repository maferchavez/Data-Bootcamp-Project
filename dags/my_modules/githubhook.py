from airflow.hooks.http_hook import HttpHook

class GithubHook(HttpHook):
    
    def __init__(self, github_conn_id):
        self.github_token = None
        conn_id = self.get_connection(github_conn_id)
        #NB Serialization object -> string, deserialization string -> object on JSON.
        if conn_id.extra_dejson('token'):
            #If the token was entered in the extras field in the airflow's connections panel, 
            self.github_token = conn_id.extra_dejason.get('token')
        #NB The super() function is used to give acces to the methods and properties of a parent or sibling class
        #NB The super() function returns an object that represents the parent class  
        super().__init__(method = 'GET', http_conn_id = github_conn_id)
    
    def get_conn(self, headers):
        """
        Accepts both Basic and Token Authentication.
        If a token exists in the "Extras" section
        with key "token", it is passed in the header.

        If not, the hook looks for a user name and password.

        In either case, it is important to ensure your privacy
        settings for your desired authentication method allow
        read access to user, org, and repo information.
        """
        if self.github_token:
            headers = {'Authorization': 'token {0}'.format(self.github_token)}
            session = super().get_conn(headers)
            session.auth = None
            return session
        return super().get_conn(headers)