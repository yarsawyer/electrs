use crate::util::errors::UnwrapPrint;

pub struct ExchangeData {
    pub bells_price: Option<f64>,
}

pub async fn get_bells_price() -> Option<f64> {
    const ERR_MSG: &str = "Failed to fetch balls price";
    const URL: &str = "https://api.coingecko.com/api/v3/simple/price?ids=bellscoin&vs_currencies=usd";
    lazy_static::lazy_static! {
        static ref CLIENT: reqwest::Client = reqwest::Client::new();
    }

    let response = CLIENT.execute(CLIENT.get(URL).build().unwrap())
        .await
        .catch(ERR_MSG)?;
    let json = serde_json::from_slice::<serde_json::Value>(
        &response.bytes().await.catch(ERR_MSG)?
    ).catch(ERR_MSG)?;
    json.get("bellscoin")
        .catch(ERR_MSG)?
        .get("usd")
        .catch(ERR_MSG)?
        .as_f64()
        .catch(ERR_MSG)
}