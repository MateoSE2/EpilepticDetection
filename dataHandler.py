import pandas as pd
import numpy as np

class DataHandler():
    def __init__(self):
        self.labels_df = None
        self.windows_matrix = None

    def read_labels_data(self, filepath):
        '''
        Llegeix les dades de les etiquetes (Excel) i les guarda com a atribut de l'objecte DataHandler com a pandas dataframe (self.labels_df).
        :param filepath: ruta del Excel
        :type filepath: str
        '''

        # read labels excel file
        pd_tmp= pd.read_excel(filepath) # conda install openpyxl
        
        # change PatID from chb01 to 1
        pd_tmp["PatID"] = pd_tmp["PatID"].apply(lambda x: int(x[3:5]))

        self.labels_df = pd_tmp

    def read_raw_data(self, filepath):
        '''
        Llegeix les dades raw del filepath, elimina les últimes 3 columnes i ho carrega en un pandas dataframe.
        Les dades raw corresponen a un sol pacient amb tots els seus recordings concatenats.

        :param filepath: ruta del arxiu parquet
        :type filepath: str
        :return: pandas de los datos del paciente, pacient_name, llista de recordings
        :rtype: pandas dataframe, str, list
        '''
        pd_pacient = pd.read_parquet(filepath, engine='pyarrow') # conda install pyarrow

        # extract pacient name
        pacient_id = int(pd_pacient["filename"][0].split("_")[0][3:5])

        # extract recordings
        list_recordings = list(set(pd_pacient["filename"]))
        list_recordings = [int(recording.split("_")[1][:-4]) for recording in list_recordings]

        return  pd_pacient


    def generate_windows(self, pd_pacient):
        """
        Separa el senyal del pacient en diferents recordings.
        Per cada recording, el separa per periodes (normal o atac (0/1)).
        També per recording el separa per finestres de k segons (128 dades per segon).
        A mesura que es creen les finestres s'etiqueten amb les metadades (label, pacient, index_inicial, periode i recording).
    
        Finalment, les metadades en un pd dataframe de tal manera que cada fila sigui una finestra amb totes les seves metadades.

        :param pd_pacient: pd amb les dades dels recordings del pacient
        :type pd_pacient: pandas dataframe
        :param pacient_name: nom del pacient
        :type pacient_name: str
        :param list_recordings: llista els noms dels recordings
        :type list_recordings: list
        """

        # split dataframe by recordings
        recordings = []
        unique_recordings = list(set(pd_pacient["filename"]))
        # list_recordings = [int(recording.split("_")[1][:-4]) for recording in list_recordings]
        for recording in unique_recordings:
            recordings.append(pd_pacient[pd_pacient["filename"] == recording])

        # remove last 3 columns
        recordings = [recording.iloc[:, :-3] for recording in recordings]

        pat_id = int(pd_pacient["filename"][0].split("_")[0][3:5])
        # slice labels by pacient
        labels_pacient = self.labels_df[self.labels_df["PatID"] == pat_id]
        periods = []
        seconds_discard = 30

        for recording,labels in zip(recordings,labels_pacient.iterrows()):
            # turn recording to numpy array
            recording = recording.to_numpy()

            if labels[1]["type"] == "seizure":
                # split by seizure
                start_seizure = labels[1]["seizure_start"]
                end_seizure = labels[1]["seizure_end"]
                
                # slice preseizure, seizure and postseizure
                periods.append((0,recording[:start_seizure-(seconds_discard*128)]))
                periods.append((1,recording[start_seizure:end_seizure]))
                periods.append((0,recording[end_seizure+(seconds_discard*128):]))
            else:
                periods.append((0,recording))

        a = 0





        


    def save_data(folder):
        """
        Guarda les windows i les metadades al folder

        :param folder: ruta del directori on guardarem la info
        :type folder: str
        """

        pass

if __name__ == "__main__":
    # Test DataHandler
    dh = DataHandler()
    dh.read_labels_data("df_annotation_full.xlsx")
    pd_pacient = dh.read_raw_data("chb01_raw_eeg_128.parquet")
    dh.generate_windows(pd_pacient)

    # Test save data
    dh.save_data("data")

