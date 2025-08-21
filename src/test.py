# Testar se os pacotes estão disponíveis
try:
    import ftfy
    from deep_translator import GoogleTranslator
    from babel.dates import format_date
    print("✅ Todos os pacotes foram importados com sucesso!")
except Exception as e:
    print("⚠️ Erro ao importar:", e)
