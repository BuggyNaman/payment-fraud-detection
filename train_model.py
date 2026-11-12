from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, DecisionTreeClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
import pandas as pd

print("Initializing Spark Session...")
spark = SparkSession.builder.appName("FraudModelComparison").getOrCreate()

print("Loading historical dataset...")
df = spark.read.csv("paysim.csv", header=True, inferSchema=True)

# Feature Engineering
indexer = StringIndexer(inputCol="type", outputCol="typeIndex", handleInvalid="keep")
feature_columns = ["amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest", "typeIndex"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features", handleInvalid="keep")

# Split data: 80% for training, 20% for testing
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

# Define the models to compare
models = {
    "Logistic Regression": LogisticRegression(labelCol="isFraud", featuresCol="features", maxIter=10),
    "Decision Tree": DecisionTreeClassifier(labelCol="isFraud", featuresCol="features", maxDepth=5),
    "Random Forest": RandomForestClassifier(labelCol="isFraud", featuresCol="features", numTrees=20, maxDepth=5)
}

# Evaluators
binary_evaluator = BinaryClassificationEvaluator(labelCol="isFraud", metricName="areaUnderROC")
multi_evaluator = MulticlassClassificationEvaluator(labelCol="isFraud", predictionCol="prediction")

results = []
best_f1 = 0
best_model_name = ""
best_model = None

print("\nTraining and evaluating models...\n")

for name, classifier in models.items():
    print(f"Training {name}...")
    pipeline = Pipeline(stages=[indexer, assembler, classifier])
    model = pipeline.fit(train_data)
    
    predictions = model.transform(test_data)
    
    # Calculate Metrics
    auc = binary_evaluator.evaluate(predictions)
    f1 = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "f1"})
    precision = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedPrecision"})
    recall = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedRecall"})
    
    results.append({
        "Model": name,
        "Precision": round(precision, 4),
        "Recall": round(recall, 4),
        "F1-Score": round(f1, 4),
        "AUC-ROC": round(auc, 4)
    })
    
    # Keep track of the best model to save for the live stream
    if f1 > best_f1:
        best_f1 = f1
        best_model_name = name
        best_model = model

# Print Tabulated Results for Section V of the IEEE Paper
print("\n--- MODEL EFFICACY RESULTS (TABLE 1) ---")
results_df = pd.DataFrame(results)
print(results_df.to_string(index=False))
print("----------------------------------------\n")

print(f"Winner: {best_model_name} with F1-Score of {best_f1:.4f}")
print("Saving the best model to disk for real-time streaming...")
best_model.write().overwrite().save("best_fraud_model")