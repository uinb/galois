
use jsonrpsee::core::Error;

#[macro_export]
macro_rules! error_msg {
    ($msg:expr) => {
        jsonrpsee::types::error::ErrorObject::owned(
            jsonrpsee::types::error::ErrorCode::InternalError.code(),
            $msg,
            None::<String>,
        )
    };
    ($code:expr, $msg:expr) => {
        jsonrpsee::types::error::ErrorObject::owned($code, $msg, None::<String>)
    };
}

#[macro_export]
macro_rules! rpc_error {
    ($msg:expr) => {
        jsonrpsee::core::Error::Call(jsonrpsee::types::error::CallError::Custom(error_msg!($msg)))
    };
    ($code:expr, $msg:expr) => {
        jsonrpsee::core::Error::Call(jsonrpsee::types::error::CallError::Custom(error_msg!(
            $code, $msg
        )))
    };
}

pub struct CustomRpcError;

impl CustomRpcError {
    pub fn user_not_found() -> Error {
        rpc_error!(-32011, "user not found")
    }

    pub fn order_not_exist() -> Error {
        rpc_error!(-32012, "order not exist")
    }

    pub fn nonce_is_expired( nonce: u32) -> Error {
        rpc_error!(-32013, format!("nonce {} is expired", nonce))
    }

    pub fn nonce_is_occupied(nonce: u32) -> Error {
        rpc_error!(-32014, format!("nonce {} is occupied", nonce))
    }

    pub fn invalid_signature() -> Error {
        rpc_error!(-32015, "invalid signature")
    }
}