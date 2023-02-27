// Copyright 2021-2023 UINB Technologies Pte. Ltd.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::core::{Currency, Symbol};
use async_std::sync::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub type Metadata = Arc<RwLock<OnchainMetadata>>;

#[derive(Clone)]
pub struct OnchainCurrency {}

#[derive(Clone)]
pub struct OnchainSymbol {}

#[derive(Clone)]
pub struct OnchainMetadata {
    symbols: HashMap<Symbol, OnchainSymbol>,
    currencies: HashMap<Currency, OnchainCurrency>,
}

impl OnchainMetadata {}
