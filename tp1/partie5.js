//5.1.1
db.transactions.aggregate([
  {
    $group: {
      _id: "$Card_Type",
      montant_total: { $sum: { $toDouble: "$Transaction_Amount (in Million)" } },
      montant_moyen: { $avg: { $toDouble: "$Transaction_Amount (in Million)" } },
      nb_transactions: { $sum: 1 }
    }
  },
  { $sort: { montant_total: -1 } }
])

//5.1.2
db.transactions.aggregate([
  {
    $group: {
      _id: "$Merchant_Category",
      total_trans: { $sum: 1 },
      nb_fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } },
      montant_fraude_total: { 
        $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, { $toDouble: "$Transaction_Amount (in Million)" }, 0] } 
      }
    }
  },
  {
    $project: {
      taux_fraude: { $multiply: [{ $divide: ["$nb_fraudes", "$total_trans"] }, 100] },
      montant_moyen_fraude: { 
        $cond: ["$nb_fraudes", { $divide: ["$montant_fraude_total", "$nb_fraudes"] }, 0] 
      },
      total_trans: 1, nb_fraudes: 1
    }
  },
  { $match: { taux_fraude: { $gt: 10 } } }
])

//5.1.3
db.transactions.aggregate([
  {
    $group: {
      _id: "$Customer_ID",
      solde: { $first: { $toDouble: "$Account_Balance (in Million)" } },
      nb_total_trans: { $sum: 1 }
    }
  },
  { $sort: { solde: -1 } },
  { $limit: 20 }
])

//5.2.1
db.transactions.aggregate([
  {
    $group: {
      _id: { $isoWeek: { $toDate: "$Transaction_Date" } },
      total_trans: { $sum: 1 },
      nb_fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } },
      montant_fraude_total: { 
        $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, { $toDouble: "$Transaction_Amount (in Million)" }, 0] } 
      }
    }
  },
  {
    $addFields: {
      moyenne_par_fraude: { $cond: ["$nb_fraudes", { $divide: ["$montant_fraude_total", "$nb_fraudes"] }, 0] }
    }
  }
])

//5.2.2
db.transactions.aggregate([
  {
    $project: {
      Fraud_Label: 1,
      Amount: { $toDouble: "$Transaction_Amount (in Million)" },
      Groupe: {
        $let: {
          vars: { pfc: { $toInt: "$Previous_Fraud_Count" } },
          in: {
            $switch: {
              branches: [
                { case: { $eq: ["$$pfc", 0] }, then: "Groupe 1 (Propre)" },
                { case: { $and: [{ $gte: ["$$pfc", 1] }, { $lte: ["$$pfc", 2] }] }, then: "Groupe 2 (Risque modéré)" }
              ],
              default: "Groupe 3 (Haut risque)"
            }
          }
        }
      }
    }
  },
  {
    $group: {
      _id: "$Groupe",
      taux_fraude: { $avg: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } },
      montant_moyen: { $avg: "$Amount" }
    }
  }
])

//5.3.1
db.merchants.insertMany([
  { Merchant_ID: "97028", Nom: "Electro-World", Adresse: "Paris", Category: "Electronics" },
  { Merchant_ID: "27515", Nom: "ATM Central", Adresse: "Lahore", Category: "ATM" }
  //mettre les autres marcha,nds ausisoi
])

db.transactions.aggregate([
  { $match: { Fraud_Label: "Fraud" } },
  {
    $lookup: {
      from: "merchants",
      localField: "Merchant_ID",
      foreignField: "Merchant_ID",
      as: "info_marchand"
    }
  },
  { $unwind: "$info_marchand" }
])

//5.4.1
db.transactions.aggregate([
  {
    $addFields: {
      score: {
        $add: [
          { $cond: ["$Is_International_Transaction", 3, 0] },
          { $cond: ["$Is_New_Merchant", 2, 0] },
          { $cond: ["$Unusual_Time_Transaction", 2, 0] },
          { $cond: [{ $gt: [{ $toDouble: "$Distance_From_Home" }, 100] }, 2, 0] },
          { $cond: [{ $gt: [{ $toDouble: "$Transaction_Amount (in Million)" }, { $multiply: [{ $toDouble: "$Avg_Transaction_Amount (in Million)" }, 2] }] }, 3, 0] },
          { $multiply: [{ $toInt: "$Failed_Transaction_Count" }, 1] }
        ]
      }
    }
  },
  { $sort: { score: -1 } },
  { $limit: 50 },
  { $project: { Transaction_ID: 1, score: 1, Fraud_Label: 1 } }
])

//5.4.2
db.transactions.aggregate([
  {
    $group: {
      _id: "$Transaction_Date",
      nb_trans: { $sum: 1 },
      nb_fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } },
      montant_fraude_total: { 
        $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, { $toDouble: "$Transaction_Amount (in Million)" }, 0] } 
      }
    }
  },
  {
    $project: {
      date: "$_id",
      nb_trans: 1,
      nb_fraudes: 1,
      montant_fraude_total: 1,
      taux_fraude: { $multiply: [{ $divide: ["$nb_fraudes", "$nb_trans"] }, 100] }
    }
  },
  { $merge: { into: "daily_fraud_stats", whenMatched: "replace" } }
])
