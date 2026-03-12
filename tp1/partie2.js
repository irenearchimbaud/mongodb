// 2.1.1 nombre de transactions frauduleuses
db.transactions.countDocuments({Fraud_Label: "Fraud"})
/*
resultat:
2475
*/

// nombre tota de transactions
db.transactions.countDocuments()
/*
resultat:
50000
*/

// nb fraude = 2475
// total = 50000
// taux fraude = (2475 / 50000) * 100
// ≈ 4.95 %
// donc environ 5% des transactions sont frauduleuses

// 2.1.2 transactiuon montant le + eleve
db.transactions.find().sort({"Transaction_Amount (in Million)": -1}).limit(1)
// donc la transaction la plus élevée est frauduleuse

// 2.1.3 les 10 clients avec le + de transactiobn
db.transactions.aggregate([
{
 $group: {
     _id: "$Customer_ID",
     nb_transactions: {$sum: 1}
 }
},
{
 $sort: {nb_transactions: -1}
},
{
 $limit: 10
}
])

// 2.2.3 categories Clothing Electronics Restaurant
db.transactions.find({
        Merchant_Category: {$in: ["Clothing","Electronics","Restaurant"]}
},
{
    Transaction_ID:1,
    "Transaction_Amount (in Million)":1,
    Merchant_Category:1,
    Fraud_Label:1,
    _id:0
})

// 2.3.1 les transactions du customer_id "24239"
// datant du 15/01/2025
db.transactions.find({
    Customer_ID: "24239",
    Transaction_Date: "2025-01-15"
})

// 2.3.3 anonymiser les IP_Address 
// + de 3 mois
db.transactions.updateMany(
    {
        Transaction_Date: {$regex: "^2025-01"}
    },
    {
        $set: {IP_Address: "ANONYMIZED"}
    }
)

// verif
db.transactions.find({
Transaction_Date: {$regex: "^2025-01"}
}).limit(5)

// 2.4.1 déplacer les transactions frauduleuses
// avec 2 tentatives echouees ou +
db.transactions.find({
    Fraud_Label: "Fraud",
    Failed_Transaction_Count: {$gte: 2}
})

// creation collection archive_transactions
db.createCollection("archive_transactions")
//resultat:
//{ ok: 1 }


// copier ds archive
db.transactions.aggregate([
    {
        $match:{
            Fraud_Label:"Fraud",
            Failed_Transaction_Count: {$gte:2}
        }
    },
    {
        $out:"archive_transactions"
    }
])

//resultat:
//operation completed

// verif
db.archive_transactions.find().limit(5)

// suppr ds la collection principale
db.transactions.deleteMany({
    Fraud_Label:"Fraud",
    Failed_Transaction_Count: {$gte:2}
})

// verif que ca a bien suppr
db.transactions.find({
Fraud_Label:"Fraud",
Failed_Transaction_Count: {$gte:2}
})


//resultat:
//aucun doc trouvé
