import requests, json, copy, traceback, re, os, sys, functools, time
from prettytable import PrettyTable
from functools import lru_cache, partial

from loguru import logger
logger.remove()
logger.add(sys.stderr, level="INFO")

new_level = logger.level("FLAG", no=38, color="<yellow>", icon="🚩")

def main()->None:

    logger.info(f"🤖 BONJOUR !")
    SETTINGS = load_json_from_file("settings.json")

    METABASE_INSTANCES = SETTINGS.get("instances")
    DB_NAMES = SETTINGS.get("db_names")
    MANUAL_MAPPING = SETTINGS.get("patterns")

    my_comparator = Comparator(MANUAL_MAPPING)

    INSTANCES = {}
    for mb_name, mb_credentials in METABASE_INSTANCES.items():
        
        api = MetabaseAPI(mb_name, mb_credentials['URL'], mb_credentials['LOGIN'], mb_credentials['PASSWORD'], dbnames=DB_NAMES)
        api.validate_connexion()
        
        INSTANCES[mb_name] = api
        my_comparator.add_instance(api)
    
    #### ADD HERE YOUR LINES AS REQUIRED :
    #my_comparator.sync_collections_from_to("A", "B")
    #my_comparator.sync_collections_from_to("A", "C")


def logger_wraps(*, entry=True, exit=True, level="FLAG"):

    def wrapper(func):
        name = func.__name__

        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            logger_ = logger.opt(depth=1)
            if entry:
                logger_.log(level, "Entering '{}' (args={}, kwargs={})", name, args, kwargs)
            result = func(*args, **kwargs)
            if exit:
                logger_.log(level, "Exiting '{}' (result={})", name, result)
            return result

        return wrapped

    return wrapper

def load_json_from_file(filename):
    """Charge un fichier JSON s'il existe, retourne le contenu ou None si le fichier n'existe pas."""
    if os.path.exists(filename):
        try:
            with open(filename, 'r') as file:
                return json.load(file)
        except json.JSONDecodeError as e:
            logger.error(f"Erreur lors de la lecture du JSON : {e}")
        except Exception as e:
            logger.error(f"Erreur inattendue lors de l'ouverture ou de la lecture du fichier : {e}")
    else:
        logger.error("Le fichier spécifié n'existe pas.")
        return {}


class MetabaseAPI():
    def __init__(self, name, HOSTNAME, LOGIN, MDP, dbnames:list[str]) -> None:
        self.name = name
        self.DBNAMES = dbnames
        self.HOSTNAME = HOSTNAME
        self.SESSION = requests.Session()
        self.SESSION.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        self.authentification(HOSTNAME, LOGIN, MDP)
        self.need_reload = True
        self.minimal_init()

    def minimal_init(self):
        self.STRUCTURE = {"databases":{}}
        self.validate_connexion()
        self.get_version()
        self.get_databases()

    def init_structure(self):
        logger.info(f"🤖[{self.name}] Récupération de la structure ...")
        self.STRUCTURE = {"databases":{}}

        self.get_databases()        
        self.get_tables()
        self.get_fields()
        self.get_collections()
        self.get_cards()
        self.get_dashboards()
        self.need_reload = False

        os.makedirs('_exports', exist_ok=True)
        with open(f'_exports/{self.name}.json', 'w') as f:
            json.dump(self.STRUCTURE, f)

        logger.info(f"🤖[{self.name}] Structure correctement initialisée et sauvegardée ici : _exports/{self.name}.json")

    def get_version(self):
        try :
            self.PROPERTIES = self.SESSION.get(f"{self.HOSTNAME}/api/session/properties").json()
            self.VERSION = self.PROPERTIES['version']['tag']
        except Exception as e :
            logger.error(f"Impossible de récupérer la version : {e}")

    def reload_if_needed(self):
        if self.need_reload :
            self.init_structure()        

    def authentification(self, HOSTNAME, LOGIN, MDP):
        try :
            logger.info(f"Authentification sur l'instance {self.name}...")
            r = self.SESSION.post(f"{HOSTNAME}/api/session", json={"username" : LOGIN, "password" : MDP}, timeout=10)
            self.TOKEN = r.json()['id']
            self.SESSION.headers.update({'x-api-key': self.TOKEN})
            self.validate_connexion
        except Exception as e :
            logger.error(f"Connexion impossible à l'instance {self.name} : {HOSTNAME} / {e}")
            raise Exception(f"Connexion impossible à l'instance {self.name} : {HOSTNAME} / {e}")

    def print_cards(self):
        for c in self.CARDS :
            to_be_kept = c['id'] in self.TO_BE_KEPT_CARDS_IDS
            if to_be_kept : print(f" -Q- {c['id']},{to_be_kept},{c['name']}")

    def validate_connexion(self):
        try :
            r = self.SESSION.get(f"{self.HOSTNAME}/api/permissions/group", timeout=10) # 10 seconds
            if r.status_code == 200 : 
                #logger.debug(f"{self.name} - Connexion OK : {self.HOSTNAME}")
                return self.SESSION
            else : 
                raise(f"{self.name} - KO : {r.text}")
        except requests.exceptions.Timeout:
            raise(f"{self.name} - Pas de connexion. {self.HOSTNAME}")

    def get_databases(self):
        """ Récupération des bases de données et stockage des bases nommées.
        """
        #print(f"Récupération des databases...")
        self.DATABASES = self.SESSION.get(f"{self.HOSTNAME}/api/database").json()['data']

        for db in self.DATABASES :
            if db['name'] in self.DBNAMES :
                self.STRUCTURE['databases'][db['id']] = {
                    'db_name' : db['name'],
                    'details' : db
                }
            else :
                #print(f"Base {db['name']} non dans la liste {self.DBNAMES}. On passe.")
                continue

    def get_collections(self)->None :
        """ Récupération des collections à garder via l'API sur l'instance de départ
        """

        self.STRUCTURE['collections'] = {}
        _collections = self.SESSION.get(f"{self.HOSTNAME}/api/collection").json()
        
        self.COLLECTIONS = [ self.SESSION.get(f"{self.HOSTNAME}/api/collection/{c['id']}").json() for c in _collections ]
        self.TO_BE_KEPT_COLLECTIONS_IDS = []

        for c in self.COLLECTIONS :
            if '🔒' in c['name'] :
                self.TO_BE_KEPT_COLLECTIONS_IDS = self.TO_BE_KEPT_COLLECTIONS_IDS + self.trouver_collections_dependantes( c['id'] )

        for c_id in self.TO_BE_KEPT_COLLECTIONS_IDS :
            c = self.SESSION.get(f"{self.HOSTNAME}/api/collection/{c_id}").json()
            self.STRUCTURE['collections'][c_id] = {
                "name" : c["name"],
                "details" : c
            }

    def import_collection(self, collection:dict)->None:
        """ Importer dans la nouvelle instance la collection passée en paramètre. Mise à jour si existe déjà.
        """
        collection_name = collection['name']
        logger.debug(f"Importation de la collection : {collection_name} dans l'instance {self.name}...")

        URL = f"{self.HOSTNAME}/api/collection"

        if collection.get('details') : collection = collection['details']

        existing_id = collection.get('id')
        if existing_id : 
            type="PUT"
            URL += f"/{existing_id}"
            req = self.SESSION.put(URL, json=collection)
        else : 
            type="POST"
            req = self.SESSION.post(URL, json=collection)

        logger.debug(f"URL={URL}, TYPE={type}, DATA={collection}")
        if req.status_code == 200 : 
            new_id = req.json().get('id') 
            
            if existing_id:
                logger.info(f"🟢 Mise à jour de la collection '{collection_name}' (ID {collection.get('old_id')}-->{new_id}): {URL}")
            else :
                logger.info(f"🟢 Importation de la nouvelle collection '{collection_name}' (ID {collection.get('old_id')}-->{new_id}): {URL}")

                c = self.SESSION.get(f"{self.HOSTNAME}/api/collection/{new_id}").json()
                self.STRUCTURE['collections'][new_id] = {
                    "name" : c["name"],
                    "details" : c
                }

                self.need_reload = True

            return self.SESSION
        else : 
            
            raise Exception(f"🟠 WARN - Importation de la collection '{collection_name}' - KO : {req.text}")

    def import_card( self, card:dict )->None:
        """ Importer une nouvelle question ou la mettre à jour.
        """
        card_name = card['name']
        logger.debug(f"Importation de la card : {card_name} dans l'instance {self.name}.")

        URL = f"{self.HOSTNAME}/api/card"

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        if card.get('details') : card = card['details']

        existing_id = card.get('id')
        if existing_id : 
            URL += f"/{existing_id}"
            type = "PUT"
            req = self.SESSION.put(URL, json=card, headers=headers)
        else : 
            type = "POST"
            req = self.SESSION.post(URL, json=card, headers=headers)

        logger.debug(f"TYPE={type}, URL={URL}, DATA={card}, HEADERS={headers}")
        if req.status_code == 200 : 

            new_id = req.json().get('id') 
            if existing_id:
                logger.info(f"🟢 Mise à jour de la question '{card_name}' (ID {card.get('old_id')}-->{new_id}): {URL}")
            else :
                logger.info(f"🟢 Importation de la nouvelle question '{card_name}' (ID {card.get('old_id')}-->{new_id}): {URL}")

            #if new_id and not new_id==existing_id :
            #    self.need_reload = True
            return self.SESSION
        else : 
            raise Exception(f"KO : {req.text}")        

    def trouver_collections_dependantes(self, racine_id):
        collections_dependantes = []

        def parcourir_dependantes(parent_id):
            for collection in self.COLLECTIONS:
                if collection.get('parent_id') == parent_id:
                    collections_dependantes.append(collection)
                    parcourir_dependantes(collection['id'])

        parcourir_dependantes(racine_id)
        return [ racine_id ] + [ c['id'] for c in collections_dependantes ]
    
    def get_cards(self) :
        _cards = self.SESSION.get(f"{self.HOSTNAME}/api/card").json()
        cards = [ self.SESSION.get(f"{self.HOSTNAME}/api/card/{c['id']}").json() for c in _cards ]

        for c in cards :
            _collection_id = c.get('collection_id')
            if _collection_id and self.STRUCTURE['collections'].get(_collection_id) :
                if not self.STRUCTURE['collections'][_collection_id].get("cards") :
                    self.STRUCTURE['collections'][_collection_id]['cards'] = {}
                self.STRUCTURE['collections'][_collection_id]['cards'][c['id']] = {
                    "name" : c['name'],
                    "details" : c
                }

    def get_tables(self):
        """ Récupération de toutes les tables, et rangement dans la super structure
        """

        #print(f"Récupération des tables ...")
        databases_ids = self.STRUCTURE["databases"].keys()
        
        self.TABLES = self.SESSION.get(f"{self.HOSTNAME}/api/table").json()
        for t in self.TABLES :
            if t['db_id'] in databases_ids :
                if not self.STRUCTURE["databases"][t['db_id']].get("tables") : self.STRUCTURE["databases"][t['db_id']]["tables"] = {}
                self.STRUCTURE["databases"][t['db_id']]["tables"][t['id']] = {
                    "name" : t['display_name'],
                    "details" : t
                }
    
    def get_fields(self):

        self.FIELDS = []
        for db_id in self.STRUCTURE["databases"].keys() :
            tables = self.STRUCTURE["databases"][db_id].get("tables") or {}
            for table_id in tables.keys() :
                metadata = self.SESSION.get(f"{self.HOSTNAME}/api/table/{table_id}/query_metadata?include_sensitive_fields=true").json()
                fields = metadata.get('fields') or []
                for field in fields :

                    if not self.STRUCTURE["databases"][db_id]["tables"][table_id].get('fields') :
                        self.STRUCTURE["databases"][db_id]["tables"][table_id]['fields']={}

                    self.STRUCTURE["databases"][db_id]["tables"][table_id]['fields'][field['id']] = {
                            "name": field['name'],
                            "details": field
                    }

                    self.FIELDS.append(field)

    def get_dashboards(self):

        dashboards_ids = []
        for collection in self.COLLECTIONS :
            _dashboards = self.SESSION.get(f"{self.HOSTNAME}/api/collection/{collection['id']}/items?models=dashboard").json()['data']
            dashboards_ids = dashboards_ids + [ d['id'] for d in _dashboards ]

        self.DASHBOARDS = [ self.SESSION.get(f"{self.HOSTNAME}/api/dashboard/{id}").json() for id in dashboards_ids ]

        for dashboard in self.DASHBOARDS :
            _collection_id = dashboard.get("collection_id")

            if not _collection_id : continue
            if not self.STRUCTURE['collections'].get(_collection_id) : continue
            if not self.STRUCTURE['collections'][_collection_id].get("dashboards") : 
                self.STRUCTURE['collections'][_collection_id]['dashboards'] = {}

            self.STRUCTURE['collections'][_collection_id]['dashboards'][ str(dashboard['id']) ] = {
                "name" : dashboard['name'],
                "details" : dashboard
            }

    def import_dashboard(self, dashboard:dict)->None:
        """ Importer une nouvelle question ou la mettre à jour.
        """
        dashboard_name = dashboard['name']
        logger.debug(f"Importation du dashboard : {dashboard_name} dans l'instance {self.name}.")

        URL = f"{self.HOSTNAME}/api/dashboard"

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        if dashboard.get('details') : dashboard = dashboard['details']

        if dashboard.get('created_at') : dashboard.pop('created_at')
        if dashboard.get('updated_at') : dashboard.pop('updated_at')

        if dashboard.get('tabs') :
            for tab in dashboard['tabs'] :
                tab.pop('entity_id')

        for label in ['param_values', 'entity_id', 'last-edit-info'] :
            if dashboard.get(label):
                dashboard.pop(label,None)

        existing_id = dashboard.get('id')
        if existing_id : 
            URL += f"/{existing_id}"
            type = "PUT"
            req = self.SESSION.put(URL, json=dashboard, headers=headers)
        else : 
            type = "POST"
            req = self.SESSION.post(URL, json=dashboard, headers=headers)

        logger.debug(f"TYPE={type}, URL={URL}, DATA={dashboard}, HEADERS={headers}")

        if req.status_code == 200 : 

            new_id = req.json().get('id') 
            fresh_dashboard = self.SESSION.get(f"{self.HOSTNAME}/api/dashboard/{new_id}").json()
            
            # Vérification : 
            if len( fresh_dashboard.get('dashcards') or [] ) == 0 :
                logger.warning(f"🟠 WARN - KO EMPTY - Le dashboard récemment ajouté ({new_id}) est vide !")
                raise Exception(f"🟠 WARN - Importation du dashboard '{dashboard_name}' - KO EMPTY : {req.text}") 

            if existing_id:
                logger.info(f"🟢 Mise à jour du dashboard '{dashboard_name}' (ID {dashboard.get('old_id')}-->{new_id}): {URL}")
            else :
                logger.info(f"🟢 Importation du nouveau dashboard '{dashboard_name}' (ID {dashboard.get('old_id')}-->{new_id}): {URL}")
            

            ############################################# Gestion des cartes
            #for _card in _dashcards :
            #    card = {
            #        "cardId":_card.get('card_id'),
            #        "col":_card.get('col'),
            #        "row":_card.get('row'),
            #        "size_x":_card.get('size_x'),
            #        "size_y":_card.get('size_y'),
            #        "series":_card.get('series'),
            #        "parameter_mappings":_card.get('parameter_mappings'),
            #        "visualization_settings":_card.get('visualization_settings')
            #    }
            #    req = self.SESSION.post(f"{self.HOSTNAME}/api/dashboard/{new_id}/cards", json=card).json()
            #    logger.info(f"Ajout de la carte {_card.get('card_id')} : {card}")
            #    logger.info(f"{req}")

            fresh_dashboard_collection_id = fresh_dashboard.get("collection_id")
            if fresh_dashboard_collection_id :
                self.STRUCTURE['collections'][fresh_dashboard_collection_id]['dashboards'][new_id] = {
                "name" : fresh_dashboard['name'],
                "details" : fresh_dashboard
            }

            return self.SESSION
         
        raise Exception(f"🟠 WARN - Importation du dashboard '{dashboard_name}' - KO : {req.text}")     
        
    def reset_all_databases_caches(self):
        logger.info(f"Reset du cache de Metabase de l'instance {self.name}")
        for db_id in self.STRUCTURE['databases'].keys():
            self.reset_db_cache(db_id)
        time.sleep(5)
    
    def reset_db_cache(self, db_id):

        URL = f"{self.HOSTNAME}/api/database/{db_id}/"

        for OPERATION in [  "discard_values", "sync_schema", "rescan_values"] :
            req = self.SESSION.post(f"{URL}{OPERATION}")
            if req.status_code == 200 :
                logger.info(f"🟢 DB[{db_id}] - {OPERATION} - OK")
            else:
                logger.warning(f"🟠 WARN - DB[{db_id}] - {OPERATION} - ERROR : {req.text()}")

class Comparator():
    def __init__(self, MANUAL_MAPPING) -> None:
        self.metabases_instances = {}
        self.MANUAL_MAPPING=MANUAL_MAPPING

    def add_instance(self, instance:MetabaseAPI):
        self.metabases_instances[instance.name] = instance.STRUCTURE
        self.metabases_instances[instance.name]['instance'] = instance

        if len(self.metabases_instances.keys())>1 :
            self.check_versions()

    def refresh_instance(self, instance_name):
        instance = self.metabases_instances[instance_name]['instance']
        instance.init_structure()

        self.metabases_instances[instance.name] = instance.STRUCTURE
        self.metabases_instances[instance.name]['instance'] = instance

    def check_versions(self):
        versions = {}
        for instance_name, instance_object in self.metabases_instances.items() :
            instance = instance_object['instance']
            versions[instance_name] = instance.VERSION
        
        if not all(value == next(iter(versions.values())) for value in versions.values()) :
            raise Exception(f"Les instances ne sont pas toutes à la même version : {versions}")
        logger.info(f"Toutes les versions sont bien identiques : {versions}")

    def clear_cache(self):
        logger.info(f"clear_cache...")
        self.get_database_id.cache_clear() 
        self.get_table_id.cache_clear() 
        self.get_field_id.cache_clear() 
        self.get_collection_id.cache_clear() 
        self.get_dashboard_id.cache_clear() 
        self.get_card_id.cache_clear()

    def reload_if_needed(self, database_name):
        if self.metabases_instances[database_name]['instance'].need_reload :
            #self.metabases_instances[database_name]['instance'].reload_if_needed()
            self.clear_cache()

    def get_database_id(self, src_database_name, src_database_id, trg_database_name ) :
        src_db_name = self.metabases_instances[src_database_name]["databases"][src_database_id]['db_name']
        trg_databases = self.metabases_instances[trg_database_name]["databases"]
        for db_id in trg_databases.keys() :
            if self.metabases_instances[trg_database_name]["databases"][db_id]['db_name']==src_db_name :
                return db_id

    def get_table_id(self, src_database_name, src_table_id, trg_database_name ) :
        """ Prend en paramètre le nom de l'instance source, un ID de table dans l'instance et l'id de l'instance cible. 
            Retourne l'ID correspondant à la table mais dans la base de donnée cible.
        """
        #print(f"get_table_id(src_database_name={src_database_name},src_table_id={src_table_id},trg_database_name={trg_database_name})")       
        src_table = None
        for db_id in self.metabases_instances[src_database_name]["databases"].keys() :
            if self.metabases_instances[src_database_name]["databases"][db_id]["tables"].get(src_table_id) :
                src_table = self.metabases_instances[src_database_name]["databases"][db_id]["tables"][src_table_id]
                break
        if not src_table : return None
        
        trg_db_id = self.get_database_id(src_database_name, db_id, trg_database_name)
        trg_tables = self.metabases_instances[trg_database_name]["databases"][trg_db_id]['tables']
        for table_id in trg_tables.keys():
            if trg_tables[table_id]['name'] == src_table['name'] :
                return table_id
        return None

    def get_field_id(self, src_database_name, src_field_id, trg_database_name ) :
        src_field = None

        for db_id in self.metabases_instances[src_database_name]["databases"].keys() :
            for table_id in self.metabases_instances[src_database_name]["databases"][db_id]["tables"].keys():
                if self.metabases_instances[src_database_name]["databases"][db_id]["tables"][table_id]["fields"].get(src_field_id) :
                    src_field = self.metabases_instances[src_database_name]["databases"][db_id]["tables"][table_id]["fields"][src_field_id]
                    src_table_id = table_id
                    src_db_id = db_id
                    break
        if not src_field : return None

        trg_db_id = self.get_database_id(src_database_name, src_db_id, trg_database_name)
        trg_table_id = self.get_table_id(src_database_name, src_table_id, trg_database_name)
        if not trg_db_id or not trg_table_id : return None
        trg_fields = self.metabases_instances[trg_database_name]["databases"][trg_db_id]["tables"][trg_table_id]["fields"]
        for field_id in trg_fields.keys() :
            if trg_fields[field_id]["name"] == src_field['name'] :
                return field_id
        return None
    
    #@logger_wraps()
    #@lru_cache(maxsize=None)
    def get_collection_id(self, src_database_name, src_collection_id, trg_database_name ) :
        src_collection = self.metabases_instances[src_database_name]["collections"].get(src_collection_id)
        if not src_collection : return None

        src_collection_name = src_collection['name']

        for collection_id in self.metabases_instances[trg_database_name]['collections'].keys():
            if self.metabases_instances[trg_database_name]['collections'][collection_id]['name'] == src_collection_name :
                return collection_id
        return None

    def get_dashboard_id(self, src_database_name, src_dashboard_id, trg_database_name):
        ## Récupération du dashboard de base dans la base de données source
        src_dashboard = None
        src_dashboard_id = str(src_dashboard_id)

        collections_ids = self.metabases_instances[src_database_name]["collections"].keys()
        for collection_id in collections_ids:
            dashboards = self.metabases_instances[src_database_name]["collections"][collection_id].get('dashboards') or {}
            dashboards_ids = [ str(id) for id in dashboards.keys() ]
            #print(f"Looking for {src_dashboard_id} in {dashboards.keys()}/{dashboards_ids} ...")
            if str(src_dashboard_id) in dashboards_ids :
                src_dashboard = dashboards.get(src_dashboard_id) or dashboards.get(str(src_dashboard_id)) or dashboards.get(int(src_dashboard_id)) #CACA
                src_collection_id = collection_id
                break
        
        if not src_dashboard : 
            logger.warning(f"🟠 WARN - get_dashboard_id(src_dashboard_id={src_dashboard_id}) --> Pas de carte avec l'ID {src_dashboard_id} dans l'instance source {src_database_name}. On veut migrer un truc qui n'existe pas ?!")
            return None

        trg_collection_id = self.get_collection_id(src_database_name, src_collection_id, trg_database_name)
        if not trg_collection_id :
            logger.warning(f"Impossible de convertir l'id de collection {src_collection_id} depuis {src_database_name} vers {trg_database_name} : la collection n'est pas dans la cible.")
            return None
        dashboards = self.metabases_instances[trg_database_name]['collections'][trg_collection_id].get('dashboards') or {}
        for dashboard_id in dashboards.keys() :
            if dashboards[dashboard_id]['name'] == src_dashboard['name'] :
                return dashboard_id

    def get_card_id(self, src_database_name, src_card_id, trg_database_name):
        """ Prend en paramètre le nom de l'instance source, un ID de card dans l'instance et l'id de l'instance cible. 
            Retourne l'ID correspondant à la carte mais dans la base de donnée cible.
        """

        ## Récupération de la carte de base dans la base de données source
        src_card = None
        src_card_id = str(src_card_id)
        
        collections_ids = self.metabases_instances[src_database_name]["collections"].keys()
        for collection_id in collections_ids:
            cards = self.metabases_instances[src_database_name]["collections"][collection_id].get('cards') or {}
            cards_ids = [ str(id) for id in cards.keys() ]
            #print(f"Looking for {src_card_id} in {cards.keys()}/{cards_ids} ...")
            if str(src_card_id) in cards_ids :
                src_card = cards.get(src_card_id) or cards.get(str(src_card_id)) or cards.get(int(src_card_id)) #CACA
                src_collection_id = collection_id
                break

        if not src_card : 
            logger.warning(f"🟠 WARN - get_card_id(src_card_id={src_card_id}) --> Pas de carte avec l'ID {src_card_id} dans l'instance source {src_database_name}. On veut migrer un truc qui n'existe pas ?!")
            return None

        trg_collection_id = self.get_collection_id(src_database_name, src_collection_id, trg_database_name)
        if not trg_collection_id : 
            logger.warning(f"get_card_id(src_card_id={src_card_id}) --> not trg_collection_id !")
            return None
        cards = self.metabases_instances[trg_database_name]['collections'][trg_collection_id].get('cards') or {}
        for card_id in cards.keys() :
            if cards[card_id]['name'] == src_card['name'] :
                return card_id

    def convert_card(self, src_database_name, data, trg_database_name):
        _data = copy.copy(data)
        if _data.get('details') : _data=_data['details']
        if _data['id'] : 
            _data['old_id'] = _data['id']
            _data['id']=self.get_card_id(src_database_name,_data['id'], trg_database_name)
        self._convert_card(src_database_name, _data, trg_database_name)
        return _data
    
    def convert_dashboard(self, src_database_name, data, trg_database_name):
        _data = copy.copy(data)
        if _data.get('details') : _data=_data['details']
        if _data['id'] : 
            _data['old_id'] = _data['id']
            _data['id']=self.get_dashboard_id(src_database_name,_data['id'], trg_database_name)
        self._convert_card(src_database_name, _data, trg_database_name)
        return _data        

    def _convert_card(self, src_database_name, data, trg_database_name):
        """ Change les champs du card passé en paramètre pour s'adapter à la prochaine instance MB.
        """
        convert_database_id = partial(self.get_database_id, src_database_name, trg_database_name=trg_database_name)
        convert_table_id = partial(self.get_table_id, src_database_name, trg_database_name=trg_database_name)
        convert_field_id = partial(self.get_field_id, src_database_name, trg_database_name=trg_database_name)
        convert_collection_id = partial(self.get_collection_id, src_database_name, trg_database_name=trg_database_name)
        convert_card_id = partial(self.get_card_id, src_database_name, trg_database_name=trg_database_name)

        if isinstance(data, dict):
            for key, value in list(data.items()):

                # Gérer les cas des ID seuls
                if key == 'id' and isinstance(value, int):
                    if "field_ref" in list(data.keys()) :
                        data[key] = convert_field_id(value)
                else:
                    # Traitement général pour d'autres IDs

                    if key in ['created_at','updated_at'] :
                        data[key] = None

                    elif (    "id" in key or 
                            'source-' in key or 
                            key in ['database'] ):  # Si la clé contient 'id', déterminer le type et remplacer
                        
                        new_id = None

                        
                        if key=="source-table" and isinstance(value, str) and "card__" in value :
                            old_card_id = value.split('__')[-1]
                            new_int_id = convert_card_id(old_card_id)
                            if not new_int_id : 
                                raise Exception(f"MISSING-TABLE - Cette question dépend d'une table inconnue ({value})") 
                            new_id = f"card__{ convert_card_id(old_card_id) }"
                        
                        elif not isinstance(value, int|str):
                            #print(f"On essaie de transformer un truc bizarre --> {key}:{value}, on le laisse comme ça.")
                            new_id = value
                        elif "database" in key:
                            new_id = convert_database_id(value)
                        elif "table" in key:
                            new_id = convert_table_id(value)
                        elif "field" in key:
                            new_id = convert_field_id(value)
                        elif "collection" in key:
                            new_id = convert_collection_id(value)
                        elif "card" in key:
                            new_id = convert_card_id(value)

                        if new_id is not None:
                            data[key] = new_id  # Mettre à jour l'ID
                    else:
                        # Recursivement remplacer les ids dans les sous-dictionnaires ou les listes
                        self._convert_card(src_database_name, value, trg_database_name)

        elif isinstance(data, list) and len(data)>1 and data[0] == "field" and isinstance(data[1], int) :
            field_id = data[1]
            new_id = convert_field_id(field_id)  # Obtenir le nouvel ID du champ
            data[1] = new_id  # Mettre à jour l'ID dans la liste

            for v in data :
                if isinstance(v, dict) or isinstance(v, list) :
                    self._convert_card(src_database_name, v, trg_database_name)

        elif isinstance(data, list):
            for idx, item in enumerate(data):

                if isinstance(item,str) and any( [ pattern in item for pattern in self.MANUAL_MAPPING.keys() ] ) :
                    
                    for pattern in self.MANUAL_MAPPING.keys() :
                        if pattern in item :
                            new_item = item.replace(pattern, self.MANUAL_MAPPING[pattern][trg_database_name])
                            data[idx] = new_item
                            logger.info(f"🫑 remplacement de {item} par {new_item}")
                            continue

                    #new_value =  self.MANUAL_MAPPING[item].get(trg_database_name)
                    #data[idx] = new_value or data[idx]
                    #continue
                
                self._convert_card(src_database_name, item, trg_database_name)

    def convert_collection(self, src_database_name, data, trg_database_name):

        _data = copy.copy(data)
        if _data.get('details') : _data=_data['details']

        _old_id = _data['id']
        if not _old_id : logger.warning(f"Cette collection n'a pas d'ID ?! : {data}")
        _old_parent_id = _data.get('parent_id')
        _new_id = self.get_collection_id(src_database_name,_old_id, trg_database_name)
        _new_parent_id = self.get_collection_id(src_database_name, _old_parent_id, trg_database_name)

        if _old_parent_id and not _new_parent_id :
                raise Exception(f"On ne peut pas migrer cette collection ({_data['name']}[{_old_id}]) car on n'a pas sa collection parente ({_old_parent_id}).")

        _data['id'] = _new_id
        _data['old_id'] = _old_id
        _data['old_parent_id'] = _old_parent_id
        _data['parent_id'] = _new_parent_id

        _data["color"] =  "#0080ff" # Verrue à la suite d'une mise à jour.
                
        logger.debug(f"convert_collection(old={_data['old_id']}, new={_data['id']}, old_parent_id={_data['old_parent_id']}, new_parent_id={_data.get('parent_id')})")
        return _data

    def sync_collections_from_to(self, src_database_name, trg_database_name):

        first_try = True
        need_retry = False
        dashboard_is_empty = 0
        
        while first_try or need_retry :
            first_try = False
            if need_retry :
                logger.info(f"🤖 A priori, ça vaut le coup de réessayer. Alors go !")
            
            self.refresh_instance(trg_database_name)
            self.refresh_instance(src_database_name)

            need_retry = False

            cards_count = dashboards_count = collections_count = 0

            collections = self.metabases_instances[src_database_name]['collections'].items()
            collections_count = len(collections)
            cards_migrated_count = dashboards_migrated_count = collections_migrated_count = 0

            for collection_id, collection in collections :
                try :
                    collection = self.convert_collection(src_database_name, collection, trg_database_name)
                    self.metabases_instances[trg_database_name]['instance'].import_collection(collection)
                    collections_migrated_count = collections_migrated_count+1
                
                except Exception as e :
                    if not "MISSING-TABLE" in str(e) :
                        logger.debug(f"Avant conversion : collection={collection}")
                    else :
                        need_retry = True
                    logger.warning(f"🟠 WARN - Problème sur la collection {collection_id} de la base {src_database_name} : {e}")
                    continue
                
                cards = self.metabases_instances[src_database_name]['collections'][collection_id].get('cards') or {}
                cards_count = cards_count + len(cards)
                
                for card_id, card in cards.items() :
                    try :
                        card = self.convert_card(src_database_name, card, trg_database_name)
                        self.metabases_instances[trg_database_name]['instance'].import_card(card)
                        cards_migrated_count = cards_migrated_count + 1
                    except Exception as e :
                        if not "MISSING-TABLE" in str(e) :
                            logger.debug(f"DEBUG : avant conversion : card={card}")
                            logger.debug(traceback.format_exc())
                        else :
                            need_retry = True                            
                        logger.warning(f"🟠 WARN - Impossible de migrer la question {card_id} : {e}")
                        #break
                        continue

            for collection_id, collection in self.metabases_instances[src_database_name]['collections'].items() : 
                dashboards = self.metabases_instances[src_database_name]['collections'][collection_id].get('dashboards') or {}
                
                dashboards_count = dashboards_count + len(dashboards.items())
                
                for dashboard_id, dashboard in dashboards.items() :
                    try :
                        dashboard = self.convert_dashboard(src_database_name, dashboard, trg_database_name)
                        self.metabases_instances[trg_database_name]['instance'].import_dashboard(dashboard)
                        self.reload_if_needed(trg_database_name)
                        dashboards_migrated_count = dashboards_migrated_count + 1
                    except Exception as e :
                        if "EMPTY" in str(e) :
                            dashboard_is_empty = dashboard_is_empty + 1
                            if dashboard_is_empty < 2 :
                                need_retry = True
                            else :
                                logger.error(f"Deux fois que le dashboard est vide, on n'insiste pas.")
                        elif "MISSING-TABLE" in str(e) :
                            need_retry = True    
                        else :
                            logger.debug(f"DEBUG : avant conversion : {dashboard}")
                            logger.debug(traceback.format_exc())
                            logger.warning(f"🟠 WARN - Impossible de migrer le dashboard {dashboard_id} : {e}")
                            break
                        continue

        logger.info(f"{collections_migrated_count} collections migrées sur {collections_count}")
        logger.info(f"{cards_migrated_count} cards migrées sur {cards_count}")
        logger.info(f"{dashboards_migrated_count} dashboards migrées sur {dashboards_count}")
        
    def print_structures(self, master_instance_name):
        headers = ["Type","Nom"]
        instances_names = list(self.metabases_instances.keys())

        if not master_instance_name in instances_names : raise Exception(f"Instance {master_instance_name} en dehors de la liste : {instances_names}")

        other_instances_names = copy.copy(instances_names)
        other_instances_names.remove(master_instance_name)

        orderred_other_instances_names = [master_instance_name]+other_instances_names

        for instance_name in orderred_other_instances_names :
            headers.append(f"{instance_name}_id")
        
        tab = PrettyTable(headers)

        for db_id in self.metabases_instances[master_instance_name]['databases'].keys():
            row = [ "database", self.metabases_instances[master_instance_name]['databases'][db_id]['db_name'], db_id ]
            for instance_name in other_instances_names :
                row.append( self.get_database_id(master_instance_name, db_id, instance_name) )
            tab.add_row(row)

            tables = self.metabases_instances[master_instance_name]['databases'][db_id]['tables']
            for table_id in tables.keys():
                row = [ "+ table", tables[table_id]['name'], table_id ]
                for instance_name in other_instances_names :
                    row.append( self.get_table_id(master_instance_name, table_id, instance_name) )
        
                tab.add_row(row)

                fields = tables[table_id]['fields']
                for field_id in fields.keys():
                    row = [ "++ champ", fields[field_id]['name'], field_id ]
                    for instance_name in other_instances_names :
                        row.append( self.get_field_id(master_instance_name, field_id, instance_name) )
                    
                    tab.add_row(row)
        
        for collection_id in self.metabases_instances[master_instance_name]['collections'].keys():
            row = [ "collection", self.metabases_instances[master_instance_name]['collections'][collection_id]['name'], collection_id ]
            for instance_name in other_instances_names :
                row.append( self.get_collection_id(master_instance_name, collection_id, instance_name) )
            tab.add_row(row)

            cards = self.metabases_instances[master_instance_name]['collections'][collection_id].get('cards') or {}
            for card_id in cards.keys():
                row = [ "+ question", cards[card_id]['name'], card_id ]
                for instance_name in other_instances_names :
                    row.append( self.get_card_id(master_instance_name, card_id, instance_name) ) 
                tab.add_row(row)              

            dashboards = self.metabases_instances[master_instance_name]['collections'][collection_id].get('dashboards') or {}
            for dashboard_id in dashboards.keys():
                row = [ "+ dashboard", dashboards[dashboard_id]['name'], dashboard_id ]
                for instance_name in other_instances_names :
                    row.append( self.get_dashboard_id(master_instance_name, dashboard_id, instance_name) )   
                tab.add_row(row)  

        print(tab)

if __name__ == "__main__":
    main()        