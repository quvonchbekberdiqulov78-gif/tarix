plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

val neuronBaseUrl = providers.gradleProperty("NEURON_BASE_URL")
    .orElse(providers.environmentVariable("NEURON_BASE_URL"))
    .orElse("https://example.invalid")
    .get()
val versionCodeValue = providers.gradleProperty("VERSION_CODE").orElse("1").get().toInt()
val versionNameValue = providers.gradleProperty("VERSION_NAME").orElse("1.0.0").get()

android {
    namespace = "uz.itkoders.neuron"
    compileSdk = 35

    defaultConfig {
        applicationId = "uz.itkoders.neuron"
        minSdk = 23
        targetSdk = 35
        versionCode = versionCodeValue
        versionName = versionNameValue
        buildConfigField("String", "NEURON_BASE_URL", "\"$neuronBaseUrl\"")
    }

    signingConfigs {
        create("release") {
            storeFile = file(System.getenv("ANDROID_KEYSTORE_PATH") ?: "missing-release.keystore")
            storePassword = System.getenv("ANDROID_KEYSTORE_PASSWORD") ?: ""
            keyAlias = System.getenv("ANDROID_KEY_ALIAS") ?: ""
            keyPassword = System.getenv("ANDROID_KEY_PASSWORD") ?: ""
        }
    }

    buildTypes {
        getByName("release") {
            signingConfig = signingConfigs.getByName("release")
            isMinifyEnabled = false
        }
    }

    buildFeatures { buildConfig = true }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions { jvmTarget = "17" }
}
