# To scrape Instagram Posts by Hashtag
import instaloader
from datetime import datetime
from itertools import dropwhile, takewhile
import csv


class GetInstagramProfile():
    def __init__(self) -> None:
        self.L = instaloader.Instaloader()

    def download_posts_with_hashtags(self, hashtag):
        SINCE = datetime(2017, 1, 1)
        UNTIL = datetime(2023, 7, 1)

        # 搜尋包含特定hashtag的貼文
        for post in instaloader.Hashtag.from_name(self.L.context, hashtag).get_posts():
            self.L.download_post(post, target='#' + hashtag)

            # 擷取tag為特定hashtag的貼文中的username
            for tagged_user in post.get_likes():
                username = tagged_user.username
                # 下載該使用者的貼文
                user_posts = instaloader.Profile.from_username(self.L.context, username).get_posts()
                for user_post in takewhile(lambda p: p.date > SINCE, dropwhile(lambda p: p.date > UNTIL, user_posts)):
                    self.L.download_post(user_post, username)

    def get_post_info_csv(self, username):
        with open(username + '.csv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            posts = instaloader.Profile.from_username(self.L.context, username).get_posts()
            for post in posts:
                print("post date: " + str(post.date))
                print("post profile: " + post.profile)
                print("post caption: " + post.caption)
                print("post location: " + str(post.location))

                posturl = "https://www.instagram.com/p/" + post.shortcode
                print("post url: " + posturl)
                writer.writerow(
                    ["post", post.mediaid, post.profile, post.caption, post.date, post.location, posturl, post.typename,
                     post.mediacount, post.caption_hashtags, post.caption_mentions, post.tagged_users, post.likes,
                     post.comments, post.title, post.url])

                print("\n\n")


if __name__ == "__main__":
    cls = GetInstagramProfile()
    cls.download_posts_with_hashtags("amr")
    cls.get_post_info_csv("test")
