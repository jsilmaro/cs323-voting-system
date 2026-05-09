from google.cloud import firestore
db = firestore.Client(project='lustrous-baton-495804-r7', database='voting-system-database')
docs = list(db.collection('votes').stream())
print(f'Total votes in Firestore: {len(docs)}')
