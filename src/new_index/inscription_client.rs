use crate::{
    inscription_entries::inscription::InscriptionContent,
    new_index::inscription_client::sha256::SHA256, util::errors::UnwrapPrint,
};

pub mod sha256 {
    pub fn sha256_hex(data: &[u8]) -> String {
        sha256::digest(data)
    }

    use primitive_types::U256;

    pub trait SHA256: AsRef<[u8]> {
        fn sha256(&self) -> Option<U256> {
            U256::from_str_radix(&sha256_hex(self.as_ref()), 16).ok()
        }
    }

    impl SHA256 for String {}
    impl SHA256 for Vec<u8> {}
}

pub async fn send_inscriptions(inscriptions: Vec<InscriptionContent>) {
    lazy_static::lazy_static! {
        static ref CLIENT: reqwest::Client = reqwest::Client::new();
        static ref URL: String = std::env::var("DUNGEON_URL")
                .expect("DUNGEON_URL must be set.");
        static ref AUTH: String = {
            let user = std::env::var("DUNGEON_MASTER")
                .expect("DUNGEON_MASTER must be set.")
                .sha256()
                .unwrap()
                .to_string();

            let password = std::env::var("THREE_HUNDRED_BUCKS")
                .expect("THREE_HUNDRED_BUCKS must be set.")
                .sha256()
                .unwrap()
                .to_string();
            format!("Basic {}", base64::encode(format!("{user}:{password}")))
        };
    };
    let body = serde_json::to_string(&inscriptions).unwrap();
    let response = CLIENT
        .execute(
            CLIENT
                .post(URL.as_str())
                .header("Content-type", "application/json")
                .header("Authorization", AUTH.as_str())
                .body(body)
                .build()
                .unwrap(),
        )
        .await;

    response.catch("Post problme");
}
