import pandas as pd
from datetime import datetime

class Logger:
    def __init__(self, file_path):
        # Define columns for the logging DataFrame
        self.columns = ['timestamp', 'message_type', 'message']
        self.log_data = pd.DataFrame(columns=self.columns)
        self.file_path = file_path
    
    def log(self, message_type, message):
        # Log a message with timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_log = pd.DataFrame([[timestamp, message_type, message]], columns=self.columns)
        self.log_data = self.log_data.append(new_log, ignore_index=True)
    
    def write_to_csv(self):
        try:
            with open(self.file_path, 'a') as f:
                self.log_data.to_csv(f, header=f.tell()==0, index=False)
        except PermissionError:
            print(f"Permission denied: Cannot write to {self.file_path}. Please close the file if it's open, or choose a different location.")
