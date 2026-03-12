//3.1.1
db.transactions.aggregate([
  { $match: { Fraud_Label: "Fraud" } },
  { 
    $group: {
      _id: { $substr: ["$Transaction_Time", 0, 2] }, 
      nb_fraudes: { $sum: 1 }
    }
  },
  { $sort: { nb_fraudes: -1 } }
])

//3.1.2
db.transactions.aggregate([
  {
    $project: {
      Customer_ID: 1,
      Fraud_Label: 1,
      Daily_Count: { $toInt: "$Daily_Transaction_Count" }
    }
  },
  {
    $match: {
      Daily_Count: { $gt: 10 },
      Fraud_Label: "Fraud"
    }
  },
  {
    $group: {
      _id: "$Customer_ID",
      total_transactions: { $max: "$Daily_Count" }
    }
  }
])

// Resultat : [ ] 
// (Dans mon export CSV le max est de 7 transactions par jour, donc la liste est vide)

//3.2.1
db.transactions.aggregate([
  {
    $group: {
      _id: "$Transaction_Location",
      total: { $sum: 1 },
      fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } }
    }
  },
  {
    $project: {
      Localisation: "$_id",
      Total_Transactions: "$total",
      Nb_Fraudes: "$fraudes",
      Taux_Fraude: { $multiply: [{ $divide: ["$fraudes", "$total"] }, 100] }
    }
  },
  { $sort: { Taux_Fraude: -1 } },
  { $limit: 5 }
])

//3.2.2
db.transactions.aggregate([
  {
    $match: {
      $expr: { $ne: ["$Transaction_Location", "$Customer_Home_Location"] }
    }
  },
  {
    $addFields: { dist: { $toDouble: "$Distance_From_Home" } }
  },
  { $match: { dist: { $gt: 200 } } },
  {
    $group: {
      _id: "Transactions > 200km",
      total: { $sum: 1 },
      fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } }
    }
  },
  {
    $project: {
      total: 1,
      taux_fraude: { $multiply: [{ $divide: ["$fraudes", "$total"] }, 100] }
    }
  }
])

//3.3.1
db.transactions.aggregate([
  { $match: { Fraud_Label: "Fraud" } },
  {
    $group: {
      _id: "$Merchant_ID",
      montant_fraude_total: { $sum: { $toDouble: "$Transaction_Amount (in Million)" } },
      nb_fraudes: { $sum: 1 }
    }
  },
  {
    $project: {
      Merchant_ID: "$_id",
      montant_fraude_total: 1,
      nb_fraudes: 1,
      montant_moyen: { $divide: ["$montant_fraude_total", "$nb_fraudes"] }
    }
  },
  { $sort: { montant_fraude_total: -1 } },
  { $limit: 10 }
])

//3.3.2
db.transactions.aggregate([
  {
    $group: {
      _id: "$Merchant_Category",
      credit: { $sum: { $cond: [{ $eq: ["$Card_Type", "Credit"] }, 1, 0] } },
      debit: { $sum: { $cond: [{ $eq: ["$Card_Type", "Debit"] }, 1, 0] } }
    }
  },
  {
    $project: {
      Category: "$_id",
      Ratio_Credit_Debit: { $divide: ["$credit", "$debit"] }
    }
  },
  { $sort: { Ratio_Credit_Debit: -1 } }
])

// grocery a le ratio le+ elevé (1.01) devant restaurant (0.99)

//3.4.1
db.transactions.aggregate([
  {
    $addFields: {
      montant: { $toDouble: "$Transaction_Amount (in Million)" },
      moyenne: { $toDouble: "$Avg_Transaction_Amount (in Million)" }
    }
  },
  {
    $match: {
      $expr: { $gt: ["$montant", { $multiply: ["$moyenne", 4] }] }
    }
  },
  {
    $group: {
      _id: null,
      total: { $sum: 1 },
      fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } }
    }
  },
  {
    $project: {
      taux_fraude: { $multiply: [{ $divide: ["$fraudes", "$total"] }, 100] }
    }
  }
])

// taux de fraude de 4.98% pr 6763 transactions

//3.4.2
db.transactions.aggregate([
  {
    $match: {
      Is_New_Merchant: true,
      Is_International_Transaction: true
    }
  },
  {
    $group: {
      _id: "$Fraud_Label",
      nb: { $sum: 1 }
    }
  }
])

// Normal: 11794, Fraude: 804. 
// taux de fraude: 6.38%, un peu + élevé que la moyenne (5%)

//3.4.3
db.transactions.aggregate([
  {
    $addFields: {
      score: {
        $add: [
          { $cond: [{ $gt: [{ $toDouble: "$Transaction_Amount (in Million)" }, { $multiply: [{ $toDouble: "$Avg_Transaction_Amount (in Million)" }, 2] }] }, 1, 0] },
          { $cond: ["$Unusual_Time_Transaction", 1, 0] },
          { $cond: ["$Is_New_Merchant", 1, 0] },
          { $cond: ["$Is_International_Transaction", 1, 0] },
          { $cond: [{ $gt: [{ $toDouble: "$Distance_From_Home" }, 100] }, 1, 0] },
          { $cond: [{ $gt: [{ $toInt: "$Daily_Transaction_Count" }, 5] }, 1, 0] }
        ]
      }
    }
  },
  { $match: { score: { $gte: 3 } } },
  {
    $group: {
      _id: null,
      total_suspects: { $sum: 1 },
      total_fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } }
    }
  },
  {
    $project: {
      total_suspects: 1,
      taux_fraude: { $multiply: [{ $divide: ["$total_fraudes", "$total_suspects"] }, 100] }
    }
  }
])

// 32814 transactions suspectes, avec un taux de fraude de 5.53%