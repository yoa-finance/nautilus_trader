#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum AllocationType {
    Unknown = 0x0_u8,
    Sor = 0x2_u8,
    NonRepresentable = 0xfe_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for AllocationType {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x0_u8 => Self::Unknown,
            0x2_u8 => Self::Sor,
            0xfe_u8 => Self::NonRepresentable,
            _ => Self::NullVal,
        }
    }
}
impl From<AllocationType> for u8 {
    #[inline]
    fn from(v: AllocationType) -> Self {
        match v {
            AllocationType::Unknown => 0x0_u8,
            AllocationType::Sor => 0x2_u8,
            AllocationType::NonRepresentable => 0xfe_u8,
            AllocationType::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for AllocationType {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "Unknown" => Ok(Self::Unknown),
            "Sor" => Ok(Self::Sor),
            "NonRepresentable" => Ok(Self::NonRepresentable),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for AllocationType {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Unknown => write!(f, "Unknown"),
            Self::Sor => write!(f, "Sor"),
            Self::NonRepresentable => write!(f, "NonRepresentable"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
