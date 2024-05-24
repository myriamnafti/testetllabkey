import re
from labkey.api_wrapper import APIWrapper
import pandas as pd
import datetime
from urllib.parse import urlparse, urlunparse
# Configuration de l'API LabKey
labkey_server = '${baseServerURL}' 
container_path = '${containerPath}'
url = urlparse(labkey_server)
context_path ='labkey'
use_ssl = False
api_key = '${apikey}'

api = APIWrapper(
    url.netloc, container_path, url.path.strip('/'),
    use_ssl=(url.scheme == 'https'), api_key=api_key)
print("URL: ",url)
print("container: ",container_path)
print("api_key: ",api_key)
print("API:", api)
def format_dates_for_labkey(series):
    if isinstance(series, str):
        return series
    elif isinstance(series, pd.Timestamp):
        return series.strftime('%Y-%m-%d %H:%M:%S')
    else:
        return pd.to_datetime(series).strftime('%Y-%m-%d %H:%M:%S')

def convert_to_date_if_possible(value):
    if isinstance(value, str):
        date_pattern = re.compile(r'\b\d{4}-\d{2}-\d{2}\b')
        # Si la valeur correspond au modele de date, la convertir en datetime
        if re.match(date_pattern, value):
            try:
                return format_dates_for_labkey(pd.to_datetime(value))
            except ValueError:
                pass
        else:
            try:
                return int(value)  # Tentative de conversion en entier
            except ValueError:
                pass
    return value
def remove_special_characters(text, dtype):
    #pattern = re.compile(r'(?<!\d),(?!\d)|[^a-zA-Z0-9\s\-,]')
    if dtype in ['int64', 'float64']:  # Verifiez si le type de donnees est numerique
        return text  # Ne pas modifier les valeurs numeriques
    else:
        pattern = re.compile(r'[^a-zA-Z0-9\s\-]')
    
        return re.sub(pattern, '', str(text))

def main():
    
    input_file_path = "${input.xls}"
    
    
    try:
        df = pd.read_excel(input_file_path)
        print('fichier input', df)
        print(df.dtypes)
    except FileNotFoundError:
        print("Le fichier d'entree n'a pas ete trouve.")
        return
    
    # Chercher les doublons sur toutes les colonnes de la DataFrame
    duplicate_rows_all_columns = df[df.duplicated(keep=False)]
    if not duplicate_rows_all_columns.empty:
        print("Duplicates found on all DataFrame columns:")
        print(duplicate_rows_all_columns)
        print("Deletion of identical rows in all columns")
        # Supprimer les lignes identiques sur toutes les colonnes
        df.drop_duplicates(inplace=True)
        print("Identical rows in all columns have been removed")
    else:
        duplicate_rows_PV = df[df.duplicated(['ParticipantId', 'SequenceNum'], keep=False)]
        if not duplicate_rows_PV.empty:
            print("Duplicates found for ParticipantId and SequenceNum columns:")
            print(duplicate_rows_PV)
            print("Unable to integrate data due to duplicates.")
        else:
            #print("No duplicates found for ParticipantId and SequenceNum columns.")

            print("No duplicates found on all DataFrame columns")

    

    # Appliquer la fonction remove_special_characters aux noms de colonnes
    df.columns = df.columns.map(lambda x: remove_special_characters(x, 'str'))

    # Appliquer la fonction remove_special_characters a toutes les cellules du DataFrame
    df = df.apply(lambda col: col.apply(lambda x: remove_special_characters(x, col.dtype)))

    print(df.columns)
    print(df.dtypes)
    
      
    # Remplacer les valeurs NaN et NaT par une chaine vide
    df.fillna('', inplace=True)
    
    
    current_date = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    dataset_name = f"${input.xls}_{current_date}".split('/')[-1]
    # Obtenir le nom du fichier sans extension
    dataset_name = dataset_name.split('.')[0]
    # Obtenir le nom du fichier sans extension ni date
    #dataset_name = dataset_name.split('_')[0]

    # Definissez new_exam_dataset_def avant la generation dynamique des champs
    new_exam_dataset_def = {
        'kind': 'StudyDatasetVisit',
        'domainDesign': {
            'name': dataset_name,
            'fields': []
        },
        'options': {
            'demographics': False  
        }
     }
    
    # Generez dynamiquement les champs a partir des colonnes de vos donnees
    fields = []
    for column_name in df.columns:
    # Determinez le type de champ en fonction du type de donnees dans la colonne
        if df[column_name].dtype == 'object':
            # Verifiez si la colonne contient des dates au format 'yyyy-mm-dd'
            if df[column_name].str.match(r'\b\d{4}-\d{2}-\d{2}\b').all():
                range_uri = 'date'
                # Convertissez toutes les valeurs en type datetime
                df[column_name] = pd.to_datetime(df[column_name])

            else:
                range_uri = 'string'  
        elif df[column_name].dtype == 'datetime64':
            range_uri = 'date'
        elif df[column_name].dtype == 'float64':
            range_uri = 'float'
        elif df[column_name].dtype == 'int64':
            range_uri = 'int'
        else:
            
            range_uri = 'string'  
        
        if column_name != 'ParticipantId' and column_name != 'SequenceNum':
            
            fields.append({
                'name': column_name,
                'label': column_name,  # Utilisez le nom de la colonne comme etiquette par defaut
                'rangeURI': range_uri
            })

    df.columns = df.columns.map(lambda x: remove_special_characters(x, df[x].dtype)) 
    print('df apres remplissage des feilds', df)
    
    # Convertir toutes les valeurs en type datetime
    #df = df.applymap(convert_to_date_if_possible)
    print(df.dtypes)
    print(df)
    # Mettez a jour new_exam_dataset_def avec les champs generes
    new_exam_dataset_def['domainDesign']['fields'] = fields

    print("Creating new dataset:",dataset_name)
    new_exam_dataset_domain = api.domain.create(new_exam_dataset_def)
    
    schema = 'study'
    table = dataset_name
    print("Inserting data into LabKey...")
    try:
        
        inserted_rows = api.query.insert_rows(schema, table, df.to_dict(orient='records'))
        print(f"{len(inserted_rows)} lignes inserees dans la table LabKey '{schema}.{table}'.")
         # Recuperer les donnees depuis LabKey
        labkey_data = api.query.select_rows(schema, table)
        labkey_df = pd.DataFrame(labkey_data['rows'])
        
        if df.equals(labkey_df):
            print("The data integrated in LabKey is identical to the input file.")
        else:
            print("The data integrated in LabKey are not identical to the input file.")
            
    except Exception as e:
        print(f"An error occurred while inserting data into LabKey : {str(e)}")




if __name__ == "__main__":
    main()