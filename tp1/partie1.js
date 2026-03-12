// 1.1.1 pour créer ma bdd: use fraudshield_banking

//1.1.2 mongoimport pour importer un fichier csv dans mongodb 

//1.1.3
// mongoimport
// --db fraudshield_banking
// --collection transactions
// --type csv
// --headerline
// --file FraudShield_Banking_Data.csv

// pour compter combien de docs
db.transactions.countDocuments()
// resultat 50000

// pr regarder un doc
db.transactions.findOne()

// les champs sont en string car les fichiers csv contiennent pas d'informations de type
// donc mongodb importe tout en string

// pr convertir:
db.transactions.updateMany(
 {},
 [
   { $set: { Transaction_ID: { $toInt: "$Transaction_ID" } } }
 ]
)

// 1.2.1
db.transactions.find().limit(5)

//1.2.2 pr convertir les yes/no en bool 
db.transactions.updateMany(
 {},
 [
  {
   $set: {
    Is_International_Transaction: {
     $cond: [
      { $eq: ["$Is_International_Transaction", "Yes"] },
      true,
      false
     ]
    }
   }
  }
 ]
)
db.transactions.updateMany(
 {},
 [
  {
   $set: {
    Is_New_Merchant: {
     $cond: [
      { $eq: ["$Is_New_Merchant", "Yes"] },
      true,
      false
     ]
    }
   }
  }
 ]
)

db.transactions.updateMany(
 {},
 [
  {
   $set: {
    Unusual_Time_Transaction: {
     $cond: [
      { $eq: ["$Unusual_Time_Transaction", "Yes"] },
      true,
      false
     ]
    }
   }
  }
 ]
)

// pr verifier que ça marche
db.transactions.find().limit(3)
