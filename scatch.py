# Get predicted probabilities for home win
confidences = pd.DataFrame({
    'game_index': X_test.index,
    'home_win_proba': rf.predict_proba(X_test)[:, 1],
    'actual_home_win': y_test
})

# Sort by confidence
confidences_sorted = confidences.sort_values('home_win_proba', ascending=False)

# Assign pick'em ranks (10 = most confident, 1 = least confident)
confidences_sorted['pickem_rank'] = range(len(confidences_sorted), 0, -1)

# Show top 10 most confident picks
print(confidences_sorted.head(10))