import re
from labkey.api_wrapper import APIWrapper
import pandas as pd
import datetime
# Configuration de l'API LabKey
labkey_server = 'localhost:8080'#'${baseServerURL}'
container_path = '${containerPath}'

context_path = 'labkey'
use_ssl = False
api_key = '${apikey}'#'862d8d6c7199713207ac1d8172796987bf7e4f7de199031133975e64addb62fe'
api = APIWrapper(labkey_server, container_path, context_path, use_ssl, api_key=api_key) 

print("URL: ",labkey_server)
print("container: ",container_path)
print("api_key: ",api_key)
print("API:", api)
def remove_special_characters(text):
    
    pattern = re.compile(r'[^a-zA-Z0-9\s-]')
    
    return re.sub(pattern, '', str(text))

def main():
    
    input_file_path = "${input.xls}"
    
    
    try:
        df = pd.read_excel(input_file_path)
    except FileNotFoundError:
        print("Le fichier d'entree n'a pas ete trouve.")
        return
    

    # Chercher les doublons pour les colonnes ParticipantId et SequenceNum 
    duplicate_rows = df[df.duplicated(['ParticipantId', 'SequenceNum'], keep=False)]
    if not duplicate_rows.empty:
        print("Des doublons ont ete trouve pour les colonnes ParticipantId et SequenceNum :")
        print(duplicate_rows)
        # Supprimer les doublons pour les colonnes ParticipantId et SequenceNum 
        df.drop_duplicates(subset=['ParticipantId', 'SequenceNum'], keep='first', inplace=True)
        print("Ils ont ete supprimes.")
    else:
        print("Aucun doublon n'a ete trouve pour les colonnes ParticipantId et SequenceNum.")

    # Appliquer la fonction remove_special_characters aux noms de colonnes
    df.columns = df.columns.map(remove_special_characters)
    # Appliquer la fonction remove_special_characters a toutes les cellules du DataFrame
    df = df.applymap(remove_special_characters)
    print(df.columns)
    print(df.dtypes)
    

    """def convert_to_date_if_possible(value):
    
     if isinstance(value, str):
        
        date_pattern = re.compile(r'\b\d{4}-\d{2}-\d{2}\b')

        # Si la valeur correspond au modele de date, la convertir en datetime
        if re.match(date_pattern, value):
            try:
                return pd.to_datetime(value)
            except ValueError:
                pass
    
    # Si la valeur ne correspond pas a un format de date, laisser telle quelle
     return value   """
    
    """try:
       df = pd.read_excel(input_file_path)
    except FileNotFoundError:
      print("Le fichier d'entree n'a pas ete trouve.")
      exit()"""
    def convert_to_date_if_possible(value):
      try:
       return pd.to_datetime(value)
      except (ValueError, TypeError):
       return value
      
    # Remplacer les valeurs NaN et NaT par une chaine vide
    df.fillna('', inplace=True)
    # Appliquer la fonction de conversion sur toutes les colonnes
    df = df.applymap(convert_to_date_if_possible)
    print('application de la conversion des colonnes')
    print(df.dtypes)
    
    

    current_date = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    dataset_name = f"${input.xls}_{current_date}".split('/')[-1]
    # Obtenir le nom du fichier sans extension
    dataset_name = dataset_name.split('.')[0]
    # Obtenir le nom du fichier sans extension ni date
    #dataset_name = dataset_name.split('_')[0]

    new_exam_dataset_def = {
	'kind': 'StudyDatasetVisit',
	'domainDesign': {
		'name': dataset_name,
		'fields': [
		
		{
			'name': 'Date',
			'label': 'Date',
			'rangeURI': 'date'
		}, {
			'name': 'StartDate',
			'label': 'StartDate',
			'rangeURI': 'date'
		}, {
			'name': 'Country',
			'label': 'Country',
			'rangeURI': 'string'
		}, {
			'name': 'PrimaryLanguage',
			'label': '',
			'rangeURI': 'string'
			}, {
			'name': 'Sex',
			'label': 'Genre',
			'rangeURI': 'string'
		}]
        },
	    'options': {
		'demographics': False  
	    }
        }

    print("Creating new dataset: pythondemodt")
    new_exam_dataset_domain = api.domain.create(new_exam_dataset_def)
    
    schema = 'study'
    table = dataset_name
    print("Insertion des donnees dans LabKey...")
    try:
        
        inserted_rows = api.query.insert_rows(schema, table, df.to_dict(orient='records'))
        print(f"{len(inserted_rows)} lignes inserees dans la table LabKey '{schema}.{table}'.")
         # Recuperer les donnees depuis LabKey
        labkey_data = api.query.select_rows(schema, table)
        labkey_df = pd.DataFrame(labkey_data['rows'])
        
        if df.equals(labkey_df):
            print("Les donnees integrees dans LabKey sont identiques au fichier d'entree.")
        else:
            print("Les donnees integrees dans LabKey ne sont pas identiques au fichier d'entree.")
            
    except Exception as e:
        print(f"Une erreur s'est produite lors de l'insertion des donnees dans LabKey : {str(e)}")




if __name__ == "__main__":
    main()
