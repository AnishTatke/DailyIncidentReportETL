from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.cluster import KMeans, DBSCAN

def get_features(df):
    numerical_features = ['Incident Hour']
    if 'Incident Year' in df.columns:
        numerical_features.append('Incident Year')
    if 'Incident Day' in df.columns:
        numerical_features.append('Incident Day')
    if 'Latitude' in df.columns:
        numerical_features.append('Latitude')
    if 'Longitude' in df.columns:
        numerical_features.append('Longitude')

    category_features = ['Incident Type', 'Incident ORI']
    if 'Incident Month' in df.columns:
        category_features.append('Incident Month')
    if 'Incident Day of Week' in df.columns:
        category_features.append('Incident Day of Week')

    return numerical_features, category_features

def create_pipeline(df):
    num_features, cat_features = get_features(df)
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), num_features),
            ('cat', OneHotEncoder(), cat_features)
        ]
    )

    pipeline = Pipeline([
        ('preprocessor', preprocessor),
        ('cluster', KMeans(n_clusters=5, random_state=42))
    ])

    return pipeline, num_features, cat_features