//4.1.1
db.transactions.find({
    "Transaction_Amount (in Million)": { $gt: 5 },
    Fraud_Label: "Fraud",
    Is_International_Transaction: true
}).explain("executionStats")

//metriques
// executionTimeMillis : 35ms
// totalDocsExamined : 50000
// nReturned : 234
// stage : "COLLSCAN"

//4.1.2
//transactiodn, dun client specifique
db.transactions.find({ Customer_ID: 24239 }).explain("executionStats") 

//marchand specifique
db.transactions.find({ Merchant_ID: 97028 }).explain("executionStats")

//par type
db.transactions.find({ Transaction_Type: "Online" }).explain("executionStats")


//4.2.1
db.transactions.createIndex({ Fraud_Label: 1 })

db.transactions.find({
 "Transaction_Amount (in Million)": { $gt: 5 },
 Fraud_Label: "Fraud",
 Is_International_Transaction: true
}).explain("executionStats")

// totalDocsExamined: 2480
// MongoDB a regardé que les fraudes au lieu des 50000 docs

//4.2.2
db.transactions.createIndex({ 
  Customer_ID: 1, 
  Transaction_Date: -1, 
  "Transaction_Amount (in Million)": 1 
})

//4.2.3
db.transactions.createIndex({ Transaction_Location: 1, Merchant_Category: 1 })
db.transactions.find({ 
  Transaction_Location: "London", 
  Merchant_Category: "Electronics" 
}).explain("executionStats")
//resultts en 0ms

//4.2.4
db.transactions.createIndex({ IP_Address: 1 }, { unique: true })

//erreur duplicate key error si doublon
// identifier les doublons et les suppr et ensuite relancer la creation

//4.3.1
db.transactions.createIndex(
  { "Transaction_Amount (in Million)": 1 },
  { partialFilterExpression: { 
      Fraud_Label: "Fraud", 
      "Transaction_Amount (in Million)": { $gt: 1 } 
    } 
  }
)

//4.3.2
db.transactions.createIndex({ Previous_Fraud_Count: 1 }, { sparse: true })

//4.3.3
db.transactions.getIndexes()
// Pour voir les stats d'utilisation
db.transactions.aggregate([ { $indexStats: {} } ])

// je peux faire : db.transactions.dropIndex("Customer_ID_1")

//4.4.1
db.transactions.createIndex({ 
  Customer_ID: 1, 
  "Transaction_Amount (in Million)": 1, 
  Transaction_Date: 1 
})
 //verif
db.transactions.find(
 { Customer_ID: 24239 },
 { Customer_ID: 1, "Transaction_Amount (in Million)": 1, Transaction_Date: 1, _id: 0 }
).explain("executionStats")

//totalDocsExamined: 0