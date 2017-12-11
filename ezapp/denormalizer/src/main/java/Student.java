import com.fasterxml.jackson.annotation.JsonProperty;

public class Student {

    private String id;
    private String teacherId;
    private String username;
    private String firstName;
    private String lastName;
    private String favoriteMovie;
    private String gender;
    private String favoriteMovieGenre;
    private String ipAddress;
    private String originCountry;
    private String address;
    private String birthDate;
    private String firstLanguage;
    private String favoriteColor;
    private String favoriteAnimal;
    private String email;
    private String phoneNumber;

    @JsonProperty("id")
    public String getId() {
        return id;
    }

    @JsonProperty("id")
    public void setId(String id) {
        this.id = id;
    }

    @JsonProperty("teacher_id")
    public String getTeacherId() {
        return teacherId;
    }

    @JsonProperty("teacher_id")
    public void setTeacherId(String id) {
        this.teacherId = id;
    }

    @JsonProperty("username")
    public String getUsername() {
        return username;
    }

    @JsonProperty("username")
    public void setUsername(String username) {
        this.username = username;
    }

    @JsonProperty("first_name")
    public String getFirstName() {
        return firstName;
    }

    @JsonProperty("first_name")
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    @JsonProperty("last_name")
    public String getLastName() {
        return lastName;
    }

    @JsonProperty("last_name")
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @JsonProperty("favorite_movie")
    public String getFavoriteMovie() {
        return favoriteMovie;
    }

    @JsonProperty("favorite_movie")
    public void setFavoriteMovie(String favoriteMovie) {
        this.favoriteMovie = favoriteMovie;
    }

    @JsonProperty("gender")
    public String getGender() {
        return gender;
    }

    @JsonProperty("gender")
    public void setGender(String gender) {
        this.gender = gender;
    }

    @JsonProperty("favorite_movie_genre")
    public String getFavoriteMovieGenre() {
        return favoriteMovieGenre;
    }

    @JsonProperty("favorite_movie_genre")
    public void setFavoriteMovieGenre(String favoriteMovieGenre) {
        this.favoriteMovieGenre = favoriteMovieGenre;
    }

    @JsonProperty("ip_address")
    public String getIpAddress() {
        return ipAddress;
    }

    @JsonProperty("ip_address")
    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    @JsonProperty("origin_country")
    public String getOriginCountry() {
        return originCountry;
    }

    @JsonProperty("origin_country")
    public void setOriginCountry(String originCountry) {
        this.originCountry = originCountry;
    }

    @JsonProperty("address")
    public String getAddress() {
        return address;
    }

    @JsonProperty("address")
    public void setAddress(String address) {
        this.address = address;
    }

    @JsonProperty("birth_date")
    public String getBirthDate() {
        return birthDate;
    }

    @JsonProperty("birth_date")
    public void setBirthDate(String birthDate) {
        this.birthDate = birthDate;
    }

    @JsonProperty("first_language")
    public String getFirstLanguage() {
        return firstLanguage;
    }

    @JsonProperty("first_language")
    public void setFirstLanguage(String firstLanguage) {
        this.firstLanguage = firstLanguage;
    }

    @JsonProperty("favorite_color")
    public String getFavoriteColor() {
        return favoriteColor;
    }

    @JsonProperty("favorite_color")
    public void setFavoriteColor(String favoriteColor) {
        this.favoriteColor = favoriteColor;
    }

    @JsonProperty("favorite_animal")
    public String getFavoriteAnimal() {
        return favoriteAnimal;
    }

    @JsonProperty("favorite_animal")
    public void setFavoriteAnimal(String favoriteAnimal) {
        this.favoriteAnimal = favoriteAnimal;
    }

    @JsonProperty("email")
    public String getEmail() {
        return email;
    }

    @JsonProperty("email")
    public void setEmail(String email) {
        this.email = email;
    }

    @JsonProperty("phone_number")
    public String getPhoneNumber() {
        return phoneNumber;
    }

    @JsonProperty("phone_number")
    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }
}
